import db
import errors
import glob
import gzip
import numpy as np
import os
import pandas as pd
import pprint
import re
import sqlite3
import sys
import util


class EVT(object):
    """Class for EVT data operations"""
    # Data columns
    all_columns = [
        "time", "pulse_width", "D1", "D2", "fsc_small", "fsc_perp", "fsc_big",
        "pe", "chl_small", "chl_big"
    ]
    all_int_columns = all_columns[:2]
    all_float_columns = all_columns[2:]

    @staticmethod
    def transform(vals):
        """Exponentiate logged SeaFlow data.

        SeaFlow data is stored as log values over 3.5 decades on a 16-bit
        linear scale. This functions exponentiates those values onto a linear
        scale from 1 to 10**3.5.

        Note: This will convert to float64
        """
        return 10**((vals / 2**16) * 3.5)

    def __init__(self, path=None, fileobj=None, read_data=True,
                 transform=False, columns=None):
        # If fileobj is set, read data from this object. The path will be used
        # to set the file name in the database and detect compression.
        self.path = path  # EVT file path, local or in S3
        self.fileobj = fileobj  # EVT data in file object

        self.headercnt = 0
        self.evt_count = 0
        self.opp_count = 0
        self.opp_evt_ratio = 0.0
        self.evt = None
        self.opp = None
        self.evt_transformed = False
        self.opp_transformed = False

        # Columns to keep. None means keep all
        if columns is None:
            self.columns = self.all_columns
        else:
            # Keep in file order
            # Only keep if name matches a real column
            self.columns = [c for c in self.all_columns if c in columns]

        # Update lists of integer and float columns
        self.int_columns = [c for c in self.all_int_columns if c in self.columns]
        self.float_columns = [c for c in self.all_float_columns if c in self.columns]

        # Set filter params to None
        # Should be set in filter()
        self.notch1 = None
        self.notch2 = None
        self.offset = None
        self.origin = None
        self.width = None

        if read_data:
            self._read_evt()

        if transform:
            self.transform_evt()

    def __repr__(self):
        keys = [
            "evt_count", "opp_count", "notch1", "notch2", "offset", "origin",
            "width", "path", "headercnt", "columns"
        ]
        return pprint.pformat({ k: getattr(self, k) for k in keys }, indent=2)

    def __str__(self):
        return self.__repr__()

    def _isgz(self):
        """Is file gzipped?"""
        return self.path and self.path.endswith(".gz")

    def get_julian_path(self):
        """Get the file path with julian directory.

        If there is no julian directory in path, just return file name. Always
        remove ".gz" extensions.
        """
        parts = parse_evt_path(self.path)
        jpath = parts["file"]
        if parts["julian"]:
            jpath = os.path.join(parts["julian"], jpath)
        if jpath.endswith(".gz"):
            jpath = jpath[:-len(".gz")]
        return jpath

    def _open(self):
        """Return an EVT file-like object for reading."""
        handle = None
        if self.fileobj:
            if self._isgz():
                handle = gzip.GzipFile(fileobj=self.fileobj)
            else:
                handle = self.fileobj
        else:
            if self._isgz():
                handle = gzip.GzipFile(self.path, "rb")
            else:
                handle = open(self.path, "rb")
        return handle

    def _read_evt(self):
        """Read an EVT binary file and return a Pandas DataFrame."""
        with self._open() as fh:
            # Particle count (rows of data) is stored in an initial 32-bit
            # unsigned int
            buff = fh.read(4)
            if len(buff) == 0:
                raise errors.EVTFileError("File is empty")
            if len(buff) != 4:
                raise errors.EVTFileError("File has invalid particle count header")
            rowcnt = np.fromstring(buff, dtype="uint32", count=1)[0]
            if rowcnt == 0:
                raise errors.EVTFileError("File has no particle data")
            # Read the rest of the data. Each particle has 12 unsigned
            # 16-bit ints in a row.
            expected_bytes = rowcnt * 12 * 2  # rowcnt * 12 columns * 2 bytes
            buff = fh.read(expected_bytes)
            if len(buff) != expected_bytes:
                raise errors.EVTFileError(
                    "File has incorrect number of data bytes. Expected %i, saw %i" %
                    (expected_bytes, len(buff))
                )
            particles = np.fromstring(buff, dtype="uint16", count=rowcnt*12)
            # Reshape into a matrix of 12 columns and one row per particle
            particles = np.reshape(particles, [rowcnt, 12])
            # Create a Pandas DataFrame with descriptive column names.
            #
            # The first two uint16s [0,10] from start of each row are left out.
            # These ints are an idiosyncrasy of LabVIEW's binary output format.
            # I believe they're supposed to serve as EOL signals (NULL,
            # linefeed in ASCII), but because the last line doesn't have them
            # it's easier to treat them as leading ints on each line after the
            # header.
            self.evt = pd.DataFrame(np.delete(particles, [0, 1], 1),
                                    columns=self.all_columns)
            # Keep a subset of columns
            if self.columns != self.all_columns:
                todrop = [c for c in self.all_columns if c not in self.columns]
                self.evt = self.evt.drop(todrop, axis=1)

            # Convert to float32
            self.evt = self.evt.astype(np.float32)

            # Record the original number of particles
            self.evt_count = len(self.evt.index)

            # Record the number of particles reported in the header
            self.headercnt = rowcnt

    def erase_evt(self):
        """Erase the evt particle data."""
        self.evt = None
        self.headercnt = 0
        self.evt_count = 0

    def filter(self, notch1=None, notch2=None, offset=0.0,
               origin=None, width=0.5):
        """Filter EVT particle data."""
        if self.evt is None or self.evt_count == 0:
            return

        if (width is None) or (offset is None):
            raise ValueError(
                "Must supply width and offset to EVT.filter()"
            )

        # Make sure all params are floats up front to prevent potential
        # python integer division bugs
        offset = float(offset)
        width = float(width)
        if not origin is None:
            origin = float(origin)
        if not notch1 is None:
            notch1 = float(notch1)
        if not notch2 is None:
            notch2 = float(notch2)

        # Correction for the difference in sensitivity between D1 and D2
        if origin is None:
            origin = (self.evt["D2"] - self.evt["D1"]).median()

        # Only keep particles detected by fsc_small
        opp = self.evt[self.evt["fsc_small"] > 1].copy()

        # Filter aligned particles (D1 = D2), with correction for D1 D2
        # sensitivity difference
        alignedD1 = (opp["D1"] + origin) < (opp["D2"] + (width * 10**4))
        alignedD2 = opp["D2"] < (opp["D1"] + origin + (width * 10**4))
        aligned = opp[alignedD1 & alignedD2]

        fsc_small_max = aligned["fsc_small"].max()

        if notch1 is None:
            min1 = aligned[aligned["fsc_small"] == fsc_small_max]["D1"].min()
            max1 = aligned[aligned["D1"] == min1]["fsc_small"].max()
            notch1 = max1 / (min1 + 10**4)

        if notch2 is None:
            min2 = aligned[aligned["fsc_small"] == fsc_small_max]["D2"].min()
            max2 = aligned[aligned["D2"] == min2]["fsc_small"].max()
            notch2 = max2 / (min2 + 10**4)

        # Filter focused particles (fsc_small > D + notch)
        oppD1 = aligned["fsc_small"] > ((aligned["D1"] * notch1) - (offset * 10**4))
        oppD2 = aligned["fsc_small"] > ((aligned["D2"] * notch2) - (offset * 10**4))
        opp = aligned[oppD1 & oppD2].copy()

        self.opp = opp
        self.opp_count = len(self.opp.index)
        try:
            self.opp_evt_ratio = float(self.opp_count) / self.evt_count
        except ZeroDivisionError:
            self.opp_evt_ratio = 0.0

        self.notch1 = notch1
        self.notch2 = notch2
        self.offset = offset
        self.origin = origin
        self.width = width

    def transform_evt(self):
        """Unlog EVT data in-place

        Returns a reference to the EVT DataFrame
        """
        if self.evt_count == 0:
            return
        self.evt_transformed = True
        return self.transform_particles(self.evt, inplace=True)

    def transform_opp(self):
        """Unlog OPP data in-place

        Returns a reference to the OPP DataFrame
        """
        if self.opp_count == 0:
            return
        self.opp_transformed = True
        return self.transform_particles(self.opp, inplace=True)

    def transform_particles(self, particles, inplace=False):
        """Unlog particle data.

        Arguments:
            inplace: Modify particles DataFrame in-place

        Returns:
            Reference to original particles DataFrame or to a copy
        """
        if not inplace:
            particles = particles.copy()
        if self.float_columns:
            particles[self.float_columns] = self.transform(particles[self.float_columns])
        return particles

    def calc_opp_stats(self):
        """Calculate min, max, mean for each channel of OPP data"""
        if self.opp_count == 0:
            return
        return self._calc_stats(self.opp)

    def calc_evt_stats(self):
        """Calculate min, max, mean for each channel of OPP data"""
        if self.evt_count == 0:
            return
        return self._calc_stats(self.evt)

    def _calc_stats(self, particles):
        """Calculate min, max, mean for each channel of particle data"""
        stats = {}
        df = self.transform_particles(particles)
        for channel in self.float_columns:
            stats[channel] = {
                "min": df[channel].min(),
                "max": df[channel].max(),
                "mean": df[channel].mean()
            }
        return stats

    def save_opp_to_db(self, cruise_name, filter_id, dbpath):
        """Save aggregate statistics for filtered particle data to SQLite"""
        if self.opp is None or self.evt_count == 0 or self.opp_count == 0:
            return

        vals = {
            "cruise": cruise_name, "file": self.get_julian_path(),
            "opp_count": self.opp_count, "evt_count": self.evt_count,
            "opp_evt_ratio": self.opp_evt_ratio, "notch1": self.notch1,
            "notch2": self.notch2, "offset": self.offset, "origin": self.origin,
            "width": self.width, "filter_id": filter_id
        }

        stats = self.calc_opp_stats()
        for channel in self.float_columns:
            if channel in ["D1", "D2"]:
                continue
            vals[channel + "_min"] = stats[channel]["min"]
            vals[channel + "_max"] = stats[channel]["max"]
            vals[channel + "_mean"] = stats[channel]["mean"]

        db.save_opp_stats(dbpath, vals)

    def write_opp_binary(self, outfile):
        """Write opp to LabView binary file.

        If outfile ends with ".gz", gzip compress.
        """
        if self.opp_count == 0:
            return

        # Detect gzip output
        gz = False
        if outfile.endswith(".gz"):
            gz = True
            outfile = outfile[:-3]

        with open(outfile, "wb") as fh:
            # Write 32-bit uint particle count header
            header = np.array([self.opp_count], np.uint32)
            header.tofile(fh)

            # Write particle data
            self._create_opp_for_binary().tofile(fh)

        if gz:
            util.gzip_file(outfile)

    def _create_opp_for_binary(self):
        """Return a copy of opp ready to write to binary file"""
        if self.opp is None:
            return

        # Convert back to original type
        opp = self.opp.astype(np.uint16)

        # Add leading 4 bytes to match LabViews binary format
        zeros = np.zeros([self.opp_count, 1], dtype=np.uint16)
        tens = np.copy(zeros)
        tens.fill(10)
        opp.insert(0, "tens", tens)
        opp.insert(1, "zeros", zeros)

        return opp.as_matrix()

    def write_opp_csv(self, outfile):
        """Write OPP to CSV file, no header."""
        if self.opp_count == 0:
            return
        self.opp.to_csv(outfile, sep=",", index=False, header=False)

    def write_evt_csv(self, outfile):
        """Write EVT to CSV file, no header."""
        if self.evt is None:
            return
        self.evt.to_csv(outfile, sep=",", index=False)


def is_evt(file_path):
    """Does the file specified by this path look like a valid EVT file?"""
    # EVT/OPP file name regexes
    evt_re = re.compile(
        r'^(?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}(?:\.opp|\.evt)?|\d+\.evt)'
        r'(?:\.gz)?$'
    )
    parts = parse_evt_path(file_path)
    return bool(parts["file"] and evt_re.match(parts["file"]))


def parse_evt_path(file_path):
    """Return a dict with entries for 'julian' dir and 'file' name"""
    julian_re = re.compile(r'^20\d{2}_\d{1,3}$')
    d = { "julian": None, "file": None }
    parts = util.splitpath(file_path)
    if len(parts) == 1:
        d["file"] = parts[0]
    elif len(parts) > 1:
        d["file"] = parts[-1]
        if julian_re.match(parts[-2]):
            d["julian"] = parts[-2]
    return d


def find_evt_files(root_dir):
    """Return a sorted list of all EVT or OPP file paths in root_dir."""
    files = util.find_files(root_dir)
    files = parse_evt_file_list(files)
    return sorted(files)


def parse_evt_file_list(files):
    """Filter list of files to only keep EVT files.

    If the first item in files is "-", assume input consists of lines of file
    paths read from stdin. Any further items in files will be ignored.
    """
    files_list = []
    if len(files) and files[0] == "-":
        for line in sys.stdin:
            f = line.rstrip()
            if is_evt(f):
                files_list.append(f)
    else:
        for f in files:
            if is_evt(f):
                files_list.append(f)
    return files_list

def concat_evts(evts, chunksize=500, erase=False):
    """Concatenate evt DataFrames in a list of EVT objects.

    This operation will erase the underlying EVT.evt DataFrames as they are
    added to the single concatenated DataFrame in order to limit memory usage.
    DataFrames are appended and erased in configurable chunks of the EVT list
    to balance performance with reasonable memory management.
    """
    if evts:
        evtdf = evts[0].evt.copy()
        if erase:
            evts[0].erase_evt()

        i = 1
        while i < len(evts):
            evtdf = evtdf.append([e.evt for e in evts[i:i+chunksize]])
            for j in range(i, i + chunksize):
                if j >= len(evts):
                    break
                    if erase:
                        evts[j].erase_evt()
            i += chunksize
        return evtdf
