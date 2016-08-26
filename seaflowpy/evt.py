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
        # Should be set in filter()
        self.notch1 = None
        self.notch2 = None
        self.offset = None
        self.origin = None
        self.width = None

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
        tostringdict["filter_options"] = {
            "notch1": self.notch1,
            "notch2": self.notch2,
            "offset": self.offset,
            "origin": self.origin,
            "width": self.width
        }
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
            buff = fh.read(expected_bytes)
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
        # Only keep particles detected by fsc_small, D1, D2
        keep = (self.df["fsc_small"] > 1) | (self.df["D1"] > 1) | (self.df["D2"] > 1)
        self.particle_count = len(self.df[keep].index)
        return self.df[keep]

    def filter_noise_old(self):
        """
        Old method to filter data below noise threshold.

        Only on particles with fsc_small values > 1 will be kept. This function
        does not modify the actual particle DataFrame. Sets
        self.particle_count.

        Returns:
            Reference to subset of particle DataFrame passing threshold.
        """

        if len(set(self.columns).intersection(set(["fsc_small"]))) < 1:
            raise ValueError("Can't apply noise filter without fsc_small")
        # Only keep particles detected by fsc_small
        keep = (self.df["fsc_small"] > 1)
        self.particle_count = len(self.df[keep].index)
        return self.df[keep]

    def filter(self, notch1=None, notch2=None, offset=0.0,
               origin=None, width=1.0):
        """Filter EVT particle data and return a new EVT object"""
        if not self.has_data():
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

        # Apply noise filter
        df = self.filter_noise()
        if len(df.index) == 0:
            # All data is noise filtered
            opp = None
        else:
            # Correction for the difference in sensitivity between D1 and D2
            if origin is None:
                origin = (df["D2"] - df["D1"]).median()

            # Old method
            #if origin is None:
            #    origin = (self.df["D2"] - self.df["D1"]).median()
            #df = self.filter_noise_old()

            # Filter aligned particles (D1 = D2), with correction for D1 D2
            # sensitivity difference
            alignedD1 = (df["D1"] + origin) < (df["D2"] + (width * 10**4))
            alignedD2 = df["D2"] < (df["D1"] + origin + (width * 10**4))
            aligned = df[alignedD1 & alignedD2]

            fsc_small_max = aligned["fsc_small"].max()
            if notch1 is None:
                min1 = aligned[aligned["fsc_small"] == fsc_small_max]["D1"].min()
                notch1 = fsc_small_max / (min1 - offset * 10**4)

                # double check that old code matches new code for now
                assert fsc_small_max == aligned[aligned["D1"] == min1]["fsc_small"].max()
            if notch2 is None:
                min2 = aligned[aligned["fsc_small"] == fsc_small_max]["D2"].min()
                notch2 = fsc_small_max / (min2 - offset * 10**4)

                # double check that old code matches new code for now
                assert fsc_small_max == aligned[aligned["D2"] == min2]["fsc_small"].max()


            # Filter focused particles (fsc_small > D * notch)
            oppD1 = aligned["fsc_small"] >= ((aligned["D1"] * notch1) - (offset * 10**4))
            oppD2 = aligned["fsc_small"] >= ((aligned["D2"] * notch2) - (offset * 10**4))
            oppdf = aligned[oppD1 & oppD2].copy()

            opp = self.clone_with_filtered(oppdf, notch1, notch2, offset, origin, width)

        return opp

    def clone_with_filtered(self, oppdf, notch1, notch2, offset, origin, width):
        """Clone this object with new OPP DataFrame."""
        clone = EVT(path=self.path, read_data=False, transform=False)
        clone.df = oppdf
        if clone.df is not None:
            clone.header_count = None
            clone.event_count = len(clone.df.index)  # same as OPP
            clone.particle_count = len(clone.df.index)  # number of OPP
            clone.transformed = self.transformed
            # convert from numpy.float64 for better downstream compatibility
            # e.g. with json.dumps
            clone.notch1 = float(notch1)
            clone.notch2 = float(notch2)
            clone.offset = offset
            clone.origin = origin
            clone.width = width
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

    def calc_particle_stats(self):
        """Calculate min, max, mean for each channel of particle data"""
        stats = {}
        if not self.transformed:
            df = self.transform_particles(inplace=False)
        else:
            df = self.df
        for channel in self.float_columns:
            stats[channel] = {
                "min": df[channel].min(),
                "max": df[channel].max(),
                "mean": df[channel].mean()
            }
        return stats

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

    def classify(self, pop_polys):
        if not self.transformed:
            df = self.transform(inplace=False)
        else:
            df = self.df
        df["pop"] = "unknown"
        for pop, verts in pop_polys.iteritems():
            # Convert polygon vertices for this pop into a matplotlib.Path
            path = vertstopath(verts)

            # Only test unknown particles
            todo = df["pop"] == "unknown"
            # Test still unknown particles
            pop_bool = path.contains_points(df.loc[todo, verts.columns])
            pop_idx = df.loc[todo, :].loc[pop_bool, :].index
            # Record population
            df.loc[pop_idx, "pop"] = pop
        if not self.transformed:
            self.df["pop"] = df["pop"]

    def save_opp_to_db(self, cruise_name, filter_id, dbpath):
        """Save aggregate statistics for filtered particle data to SQLite"""
        if not self.has_data():
            return

        vals = {
            "cruise": cruise_name, "file": self.file_id,
            "all_count": self.parent.event_count,
            "opp_count": self.particle_count, "evt_count": self.parent.particle_count,
            "opp_evt_ratio": self.opp_evt_ratio, "notch1": self.notch1,
            "notch2": self.notch2, "offset": self.offset, "origin": self.origin,
            "width": self.width, "filter_id": filter_id
        }

        stats = self.calc_particle_stats()
        for channel in self.float_columns:
            if channel in ["D1", "D2"]:
                continue
            vals[channel + "_min"] = stats[channel]["min"]
            vals[channel + "_max"] = stats[channel]["max"]
            vals[channel + "_mean"] = stats[channel]["mean"]

        db.save_opp_stats(dbpath, vals)

    def save_vct_to_db(self, cruise_name, gating_id, dbpath):
        """Save population statistics to SQLite"""
        if not self.has_data() or "pop" not in self.df.columns:
            return

        common_vals = {
            "cruise": cruise_name, "file": self.file_id,
            "gating_id": gating_id, "method": "Manual Gating"
        }

        stats = self.calc_pop_stats()
        for item in stats.values():
            item.update(common_vals)

        db.save_vct_stats(dbpath, stats.values())

    def write_binary(self, outdir, opp=True):
        """Write particle to LabView binary file in outdir
        """
        if not self.has_data():
            return

        # Might have julian day, might not
        root = os.path.join(outdir, os.path.dirname(self.file_id))
        util.mkdir_p(root)
        outfile = os.path.join(outdir, self.file_id)
        if opp:
             outfile += ".opp"
        if os.path.exists(outfile):
            os.remove(outfile)
        if os.path.exists(outfile + ".gz"):
            os.remove(outfile + ".gz")

        with open(outfile, "wb") as fh:
            # Write 32-bit uint particle count header
            header = np.array([self.particle_count], np.uint32)
            header.tofile(fh)

            # Write particle data
            self._create_particle_matrix().tofile(fh)

        util.gzip_file(outfile)

    def write_vct(self, outdir):
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

        return df.as_matrix()


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
