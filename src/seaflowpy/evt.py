from __future__ import division
from builtins import range
from . import db
from . import errors
import gzip
import io
import json
import numpy as np
import os
import pandas as pd
import platform
import pprint
import re
from . import seaflowfile
import sys
from . import util
from . import vct
from collections import OrderedDict


class EVT(seaflowfile.SeaFlowFile):
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
        seaflowfile.SeaFlowFile.__init__(self, path, fileobj)
        # First 4 byte uint in binary file reporting number of events
        self.header_count = 0
        # Number of events recorded in EVT file
        self.event_count = 0
        # Number of particles. On first read from file this is equal to the
        # event count. Once a noise filtered is applied this count is updated.
        self.particle_count = 0
        # Event/particle DataFrame
        self.df = None
        # Are the DataFrame float columns exponentiated?
        self.transformed = False
        # If this is filtered data, a reference to unfiltered EVT parent object
        self.parent = None

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
        # Will be set in opp objects during cloning
        self.filter_params = None

        if read_data:
            self._read_binary()

        if transform:
            self.transform_particles()

    def __str__(self):
        keys = [
            "path", "file_id", "header_count", "event_count", "particle_count",
            "columns", "transformed"
        ]
        tostringdict = OrderedDict([(k, getattr(self, k)) for k in keys])
        tostringdict["filter_params"] = self.filter_params
        return json.dumps(tostringdict, indent=2)

    def _read_binary(self):
        """Read an EVT/OPP binary file into a Pandas DataFrame."""
        with self.open() as fh:
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
            # must cast to int here because while BufferedIOReader objects
            # returned from io.open(path, "rb") will accept a numpy.int64 type,
            # io.BytesIO objects will not accept this type and will only accept
            # vanilla int types. This is true for Python 3, not for Python 2.
            buff = fh.read(int(expected_bytes))
            if len(buff) != expected_bytes:
                raise errors.EVTFileError(
                    "File has incorrect number of data bytes. Expected %i, saw %i" %
                    (expected_bytes, len(buff))
                )
            events = np.fromstring(buff, dtype="uint16", count=rowcnt*12)
            # Reshape into a matrix of 12 columns and one row per particle
            events = np.reshape(events, [rowcnt, 12])
            # Create a Pandas DataFrame with descriptive column names.
            #
            # The first two uint16s [0,10] from start of each row are left out.
            # These ints are an idiosyncrasy of LabVIEW's binary output format.
            # I believe they're supposed to serve as EOL signals (NULL,
            # linefeed in ASCII), but because the last line doesn't have them
            # it's easier to treat them as leading ints on each line after the
            # header.
            self.df = pd.DataFrame(np.delete(events, [0, 1], 1),
                                   columns=self.all_columns)
            # Keep a subset of columns
            if self.columns != self.all_columns:
                todrop = [c for c in self.all_columns if c not in self.columns]
                self.df = self.df.drop(todrop, axis=1)

            # Convert to float64
            self.df = self.df.astype(np.float64)

            # Record the original number of events read from file
            self.event_count = len(self.df.index)
            # Assume all events are real particles at first
            self.particle_count = self.event_count

            # Record the number of events reported in the header
            self.header_count = int(rowcnt)

    def has_data(self):
        """Is there event data in this object?"""
        return ((self.df is not None) and (len(self.df) > 0))

    def filter_noise(self):
        """
        Filter data below noise threshold.

        This function does not modify the actual particle DataFrame. Only
        particles with D1, D2, or fsc_small values > 1 will be kept. Sets
        self.particle_count.

        Returns:
            Reference to subset of particle DataFrame passing threshold.
        """
        if len(set(self.columns).intersection(set(["D1", "D2", "fsc_small"]))) < 3:
            raise ValueError("Can't apply noise filter without D1, D2, and fsc_small")
        # Only keep particles detected by fsc_small, D1, or D2
        keep = (self.df["fsc_small"] > 1) | (self.df["D1"] > 1) | (self.df["D2"] > 1)
        self.particle_count = len(self.df[keep].index)
        return self.df[keep]

    def filter(self, params):
        """Filter EVT particle data and return a new EVT object"""
        if not self.has_data():
            return

        # Check parameters
        param_keys = [
            "width", "notch_small_D1", "notch_small_D2", "notch_large_D1",
            "notch_large_D2", "offset_small_D1", "offset_small_D2",
            "offset_large_D1", "offset_large_D2"
        ]
        if not params:
            raise ValueError("Missing filter parameters in EVT.filter()")
        for k in param_keys:
            if not k in params:
                raise ValueError(
                    "Missing filter parameter {} in EVT.filter()".format(k)
                )
            # Make sure all params are floats up front to prevent potential
            # python integer division bugs
            try:
                value = float(params[k])
            except ValueError as e:
                raise ValueError(
                    "filter parameter {}: {} could not be converted to float".format(k, params[k])
                )
            params[k] = value

        # Apply noise filter
        df = self.filter_noise()

        # Filter for aligned/focused particles
        if len(df.index) == 0:
            # All data is noise filtered
            opp = None
        else:
            # Filter aligned particles (D1 = D2), with correction for D1 D2
            # sensitivity difference
            alignedD1 = df["D1"] < (df["D2"] + params["width"])
            alignedD2 = df["D2"] < (df["D1"] + params["width"])
            aligned = df[alignedD1 & alignedD2]

            # Filter focused particles
            opp_small_D1 = aligned["D1"] <= ((aligned["fsc_small"] * params["notch_small_D1"]) + params["offset_small_D1"])
            opp_small_D2 = aligned["D2"] <= ((aligned["fsc_small"] * params["notch_small_D2"]) + params["offset_small_D2"])
            opp_large_D1 = aligned["D1"] <= ((aligned["fsc_small"] * params["notch_large_D1"]) + params["offset_large_D1"])
            opp_large_D2 = aligned["D2"] <= ((aligned["fsc_small"] * params["notch_large_D2"]) + params["offset_large_D2"])
            oppdf = aligned[(opp_small_D1 & opp_small_D2) | (opp_large_D1 & opp_large_D2)].copy()

            opp = self.clone_with_filtered(oppdf, params)

        return opp

    def clone_with_filtered(self, oppdf, params):
        """Clone this object with new OPP DataFrame."""
        clone = EVT(path=self.path, read_data=False, transform=False)
        clone.df = oppdf
        if clone.df is not None:
            clone.header_count = None
            clone.event_count = len(clone.df.index)  # same as OPP
            clone.particle_count = len(clone.df.index)  # number of OPP
            clone.transformed = self.transformed
            clone.filter_params = params
            clone.int_columns = self.int_columns
            clone.float_columns = self.float_columns
            clone.parent = self

            try:
                clone.opp_evt_ratio = float(clone.particle_count) / clone.parent.particle_count
            except ZeroDivisionError:
                clone.opp_evt_ratio = 0.0

        return clone

    def transform_particles(self, inplace=True):
        """Unlog particle data.

        Arguments:
            inplace: Modify particles DataFrame in-place

        Returns:
            Reference to original particles DataFrame or to a copy
        """
        if inplace:
            particles = self.df
            self.transformed = True
        else:
            particles = self.df.copy()
        if self.float_columns and self.particle_count > 0:
            particles[self.float_columns] = self.transform(particles[self.float_columns])
        return particles

    def calc_pop_stats(self):
        stats = {}
        if not self.transformed:
            df = self.transform_particles(inplace=False)
        else:
            df = self.df
        if "pop" in df.columns:
            bypop = df.groupby("pop")
            means = bypop.mean()
            counts = bypop.size()
            for pop in bypop.groups:
                stats[pop] = {
                    "pop": pop,
                    "count": counts[pop]
                }
                for column in ["fsc_small", "fsc_perp", "pe", "chl_small"]:
                    if column in df.columns:
                        stats[pop][column] = means.loc[pop, column]
        else:
            raise ValueError(
                "Particle DataFrame must contain pop column to calculate population statistics"
            )
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
        self.df["pop"] = vctobj.vct["pop"]

    def save_opp_to_db(self, filter_id, quantile, dbpath):
        """Save aggregate statistics for filtered particle data to SQLite"""
        if not self.has_data():
            return

        vals = {
            "file": self.file_id,
            "all_count": self.parent.event_count,
            "opp_count": self.particle_count,
            "evt_count": self.parent.particle_count,
            "opp_evt_ratio": self.opp_evt_ratio,
            "filter_id": filter_id,
            "quantile": quantile
        }

        db.save_opp_stats(dbpath, vals)

    def write_binary(self, outdir, opp=True, quantile=None):
        """Write particle to LabView binary file in outdir
        """
        if not self.has_data():
            return

        # Might have julian day, might not
        if quantile is not None:
            root = os.path.join(outdir, str(quantile), os.path.dirname(self.file_id))
        else:
            root = os.path.join(outdir, os.path.dirname(self.file_id))
        util.mkdir_p(root)

        outfile = os.path.join(root, os.path.basename(self.file_id))

        if opp:
             outfile += ".opp"
        if os.path.exists(outfile):
            os.remove(outfile)
        if os.path.exists(outfile + ".gz"):
            os.remove(outfile + ".gz")

        # Python 2 io.open() and numpy array.tofile() don't play nice together.
        # If using Python 2 use builtin open() instead of io.open.
        fh = None
        if platform.python_version_tuple()[0] == "2":
            fh = open(outfile, "wb")
        else:
            fh = io.open(outfile, "wb")

        # Create 32-bit uint particle count header
        header = np.array([self.particle_count], np.uint32)
        try:
            # Write 32-bit uint particle count header
            header.tofile(fh)
            # Write particle data
            self._create_particle_matrix().tofile(fh)
        finally:
            # Always close filehandle if it was opened
            if fh is not None:
                fh.close()

        util.gzip_file(outfile)

    def write_vct(self, outdir, quantile):
        if "pop" not in self.df.columns:
            raise ValueError("EVT DataFrame must contain pop column to write VCT csv file")
        vct_ = vct.VCT(path=self.path, vct=self.df["pop"].values)
        vct_.write_vct(outdir)

    def _create_particle_matrix(self):
        """Return a copy of df ready to write to binary file"""
        if not self.has_data():
            return

        # Convert back to original type
        df = self.df.astype(np.uint16)

        # Add leading 4 bytes to match LabViews binary format
        zeros = np.zeros([self.particle_count, 1], dtype=np.uint16)
        tens = np.copy(zeros)
        tens.fill(10)
        df.insert(0, "tens", tens)
        df.insert(1, "zeros", zeros)

        return df.values


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
    verts_list = verts.values.tolist()
    verts_list.append(verts_list[0])  # close the polygon
    codes = [Path.MOVETO]
    for i in range(len(verts_list)-2):
        codes.append(Path.LINETO)
    codes.append(Path.CLOSEPOLY)
    return Path(verts_list, codes)
