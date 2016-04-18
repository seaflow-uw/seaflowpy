import db
import errors
import gzip
import json
import numpy as np
import os
import pandas as pd
import pprint
import re
import seaflowfile
import sys
import util
import vct
from collections import OrderedDict
from matplotlib.path import Path


class EVT(seaflowfile.SeaflowFile):
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
        seaflowfile.SeaflowFile.__init__(self, path, fileobj)
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

    def __str__(self):
        keys = ["path", "file_id", "evt_count", "opp_count", "columns"]
        tostringdict = OrderedDict([(k, getattr(self, k)) for k in keys])
        tostringdict["filter_options"] = {
            "notch1": self.notch1,
            "notch2": self.notch2,
            "offset": self.offset,
            "origin": self.origin,
            "width": self.width
        }
        return json.dumps(tostringdict, indent=2)

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

    def add_vct(self, vct):
        """Add population annotations from VCT object"""
        self.evt["pop"] = vct.vct

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

    def calc_pop_stats(self):
        stats = {}
        if "pop" in self.evt.columns:
            bypop = self.evt.groupby("pop")
            means = bypop.mean()
            counts = bypop.size()
            for pop in bypop.groups:
                stats[pop] = {
                    "pop": pop,
                    "count": counts[pop]
                }
                for column in ["fsc_small", "fsc_perp", "pe", "chl_small"]:
                    if column in self.evt.columns:
                        stats[pop][column] = means.loc[pop, column]
        else:
            raise ValueError("EVT DataFrame must contain pop column to run calculate population statistics")
        return stats

    def add_vct(self, vct_dir_or_file):
        if os.path.isdir(vct_dir_or_file):
            vct_file = os.path.join(vct_dir_or_file, self.file_id + ".vct")
            if not os.path.exists(vct_file):
                vct_file = vct_file + ".gz"
        else:
            vct_file = vct_dir_or_file

        if not os.path.exists(vct_file):
            raise IOError("VCT file for {} could not be found". format(self.file_id))

        vctobj = vct.VCT(vct_file)
        self.evt["pop"] = vctobj.vct["pop"]

    def classify(self, pop_polys):
        self.evt["pop"] = "unknown"
        for pop, verts in pop_polys.iteritems():
            # Convert polygon vertices for this pop into a matplotlib.Path
            path = vertstopath(verts)

            # Only test unknown particles
            todo = self.evt["pop"] == "unknown"
            # Test still unknown particles
            pop_bool = path.contains_points(self.evt.loc[todo, verts.columns])
            pop_idx = self.evt.loc[todo, :].loc[pop_bool, :].index
            # Record population
            self.evt.loc[pop_idx, "pop"] = pop

    def save_opp_to_db(self, cruise_name, filter_id, dbpath):
        """Save aggregate statistics for filtered particle data to SQLite"""
        if self.opp is None or self.evt_count == 0 or self.opp_count == 0:
            return

        vals = {
            "cruise": cruise_name, "file": self.file_id,
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

    def save_vct_to_db(self, cruise_name, gating_id, dbpath):
        """Save population statistics to SQLite"""
        if self.evt is None or self.evt_count == 0 or "pop" not in self.evt.columns:
            return

        common_vals = {
            "cruise": cruise_name, "file": self.file_id,
            "gating_id": gating_id, "method": "Manual Gating"
        }

        stats = self.calc_pop_stats()
        for item in stats.values():
            item.update(common_vals)

        db.save_vct_stats(dbpath, stats.values())

    def write_opp_binary(self, opp_dir):
        """Write opp to LabView binary file in opp_dir
        """
        if self.opp_count == 0:
            return

        # Might have julian day, might not
        outdir = os.path.join(opp_dir, os.path.dirname(self.file_id))
        util.mkdir_p(outdir)
        outfile = os.path.join(opp_dir, self.file_id + ".opp")
        if os.path.exists(outfile):
            os.remove(outfile)
        if os.path.exists(outfile + ".gz"):
            os.remove(outfile + ".gz")

        with open(outfile, "wb") as fh:
            # Write 32-bit uint particle count header
            header = np.array([self.opp_count], np.uint32)
            header.tofile(fh)

            # Write particle data
            self._create_opp_for_binary().tofile(fh)

        util.gzip_file(outfile)

    def write_vct_csv(self, vct_dir):
        if "pop" not in self.evt.columns:
            raise ValueError("EVT DataFrame must contain pop column to write VCT csv file")
        outfile = os.path.join(vct_dir, self.file_id + ".vct")
        util.mkdir_p(os.path.dirname(outfile))
        if os.path.exists(outfile):
            os.remove(outfile)
        if os.path.exists(outfile + ".gz"):
            os.remove(outfile + ".gz")
        with gzip.open(outfile + ".gz", "wb") as f:
            f.write("\n".join(self.evt["pop"].values) + "\n")

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


def is_evt(file_path):
    """Does the file specified by this path look like a valid EVT file?"""
    # EVT/OPP file name regexes
    evt_re = re.compile(
        r'^(?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}(?:\.evt)?(?:\.opp)?|\d+\.evt)(?:\.opp)?'
        r'(?:\.gz)?$'
    )
    parts = seaflowfile.parse_path(file_path)
    return bool(parts["file"] and evt_re.match(parts["file"]))


def find_evt_files(root_dir):
    """Return a chronologically sorted list of EVT or OPP file paths in root_dir."""
    files = util.find_files(root_dir)
    files = parse_file_list(files)
    return seaflowfile.sorted_files(files)


def parse_file_list(files):
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


def vertstopath(verts):
    """Convert polygon vertices as 2 column pandas DataFrame to a matplotlib.Path"""
    verts_list = verts.as_matrix().tolist()
    verts_list.append(verts_list[0])  # close the polygon
    codes = [Path.MOVETO]
    for i in range(len(verts_list)-2):
        codes.append(Path.LINETO)
    codes.append(Path.CLOSEPOLY)
    return Path(verts_list, codes)
