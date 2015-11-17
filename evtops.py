import numpy as np
import pandas as pd
import sys


class EVT(object):
    """Class for EVT data operations"""

    def __init__(self, file_name):
        self.file_name = file_name
        self.evtcnt = 0
        self.oppcnt = 0
        self.evt = None
        self.opp = None

        self.read_evt()

    def read_evt(self):
        """Read an EVT binary file and return a pandas DataFrame."""
        cols = ["time", "pulse_width", "D1", "D2",
                "fsc_small", "fsc_perp", "fsc_big",
                "pe", "chl_small", "chl_big"]
        with open(self.file_name) as fh:
            try:
                rowcnt = np.fromfile(fh, dtype="uint32", count=1)
                particles = np.fromfile(fh, dtype="uint16", count=rowcnt*12)
                particles = np.reshape(particles, [rowcnt, 12])
                self.evt = pd.DataFrame(np.delete(particles, [0, 1], 1),
                                        columns=cols)
                self.evt = self.evt.astype("float64")  # sqlite3 schema compat
                self.evtcnt = len(self.evt.index)
            except Exception:
                sys.stderr.write("Could not parse file %s\n" % self.file_name)

    def filter_particles(self, notch=1, width=0.5, slope=1):
        """Filter EVT particle data.

        Args:
            notch: filter notch (default 1)
            width: filter width (default 0.5)
            slope: filter slope (default 1)
        """
        # Only keep particles detected by fsc_small, D1, and D2
        detected = (self.evt["fsc_small"] > 1) & (self.evt["D1"] > 1) & \
                   (self.evt["D2"] > 1)
        # Not sure if copy() is needed, but want to make sure we don't change
        # original data
        opp = self.evt[detected].copy()

        # Only keep particles that have not saturated D1 or D2
        maxD1D2 = opp[["D1", "D2"]].max().max()
        opp = opp[(opp["D1"] < maxD1D2) & (opp["D2"] < maxD1D2)]

        # Correct for D1 D2 sensitivity difference
        origin_data = opp[(opp["D1"] > 5000) | (opp["D2"] > 5000)]
        origin = (origin_data["D2"] - origin_data["D1"]).median()
        if (origin > 0):
            opp["D1"] = opp["D1"] + origin
        elif (origin < 0):
            opp["D2"] = opp["D2"] - origin

        # Filter aligned particles (D1 = D2), with correction for D1 D2
        # sensitivity difference
        alignedD1 = opp["D1"] < (opp["D2"] * slope + (width * 10**4))
        alignedD2 = opp["D2"] < (opp["D1"] * slope + (width * 10**4))
        aligned = opp[alignedD1 & alignedD2]

        oppD1 = aligned["D1"] / aligned["fsc_small"] < notch
        oppD2 = aligned["D2"] / aligned["fsc_small"] < notch
        opp = aligned[oppD1 & oppD2].copy()

        # Back to original D1 and D2 values
        if (origin > 0):
            opp["D1"] = opp["D1"] - origin
        elif (origin < 0):
            opp["D2"] = opp["D2"] + origin

        self.opp = opp
        self.oppcnt = len(self.opp.index)

    def add_extra_columns(self, cruise_name, particles_seen):
        """Add columns for cruise name, file name, and particle ID to OPP."""
        if self.opp is None:
            sys.stderr.write("EVT must be filtered before this method\n")
        else:
            ids = range(particles_seen, particles_seen+self.oppcnt)
            self.opp.insert(0, "cruise", cruise_name)
            self.opp.insert(1, "file", self.file_name)
            self.opp.insert(2, "particle", ids)

    def write_opp_csv(self, outfile):
        self.opp.to_csv(outfile, sep=",", index=False)

    def write_evt_csv(self, outfile):
        self.evt.to_csv(outfile, sep=",", index=False)

    def write_opp_sqlite3(self, con):
        sql = "INSERT INTO opp VALUES (%s)" % ",".join("?"*self.opp.shape[1])
        con.executemany(sql, self.opp.itertuples(index=False))

    def write_opp_hdf5(self, store):
        """Save OPP data to pandas HDFStore storage"""
        store[self.file_name] = self.opp  # this most likely produces a warning
