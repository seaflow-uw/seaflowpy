#!/usr/bin/env python

from argparse import ArgumentParser
import datetime
import numpy as np
import pandas as pd
import pprint
import os
import re
import sqlite3 as sq
import sys


def main():
    p = ArgumentParser(description="Filter EVT data.")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--files", nargs="+",
                   help="""EVT file paths. - to read from stdin.
                        (required unless --evt_dir)""")
    g.add_argument("--evt_dir",
                   help="EVT directory path (required unless --files)")

    p.add_argument("--db", required=True, help="sqlite3 db file (required)")
    p.add_argument("--cruise", required=True, help="cruise name (required)")
    p.add_argument("--notch1", type=float, help="notch 1 (optional)")
    p.add_argument("--notch2", type=float, help="notch 2 (optional)")
    p.add_argument("--width", type=float, default=0.5, help="width (optional)")
    p.add_argument("--origin", type=float, help="origin (optional)")
    p.add_argument("--offset", type=float, default=0.0,
                   help="offset (optional)")
    p.add_argument("--no-index", default=False, action="store_true",
                   help="Skip creation of opp table indexes (optional)")

    args = p.parse_args()

    if args.files:
        files = parse_file_list(args.files)
    else:
        files = find_evt_files(args.evt_dir)

    # Print defined parameters
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    print "Defined parameters:"
    pprint.pprint(v, indent=2)


    filter_files(files, args.cruise, args.notch1, args.notch2, args.width,
                 args.origin, args.offset, args.db)
    if not args.no_index:
        create_indexes(args.db)


def parse_file_list(files):
    files_list = []

    if len(files) and files[0] == "-":
        for line in sys.stdin:
            files_list.append(line.rstrip())
    else:
        files_list = files

    exists = []

    for f in files_list:
        if not os.path.isfile(f):
            sys.stderr.write("%s does not exist\n" % f)
        else:
            exists.append(f)

    return exists


def find_evt_files(evt_dir):
    evt_files = []
    evt_re = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}$')

    for root, dirs, files in os.walk(evt_dir):
        for f in files:
            if evt_re.match(f):
                evt_files.append(os.path.join(root, f))

    return evt_files


def filter_files(files, cruise, notch1, notch2, width, origin, offset, dbpath):
    t0 = datetime.datetime.now()

    print ""
    print "Filtering %i EVT files" % len(files)

    ensure_opp_table(dbpath)
    ensure_opp_evt_ratio_table(dbpath)

    evtcnt = 0
    oppcnt = 0
    files_ok = 0

    for f in files:
        evt = EVT(f)

        if not evt.ok:
            continue

        evt.filter_particles(notch1=notch1, notch2=notch2, origin=origin,
                             offset=offset, width=width)

        evt.save_opp_to_db(cruise, oppcnt, dbpath)

        evtcnt += evt.evtcnt
        oppcnt += evt.oppcnt
        files_ok += 1
        print "%s: %i / %i = %.06f" % (f, evt.oppcnt, evt.evtcnt,
                                       evt.opp_evt_ratio)
    try:
        opp_evt_ratio = float(oppcnt) / evtcnt
    except ZeroDivisionError:
        opp_evt_ratio = 0.0

    t1 = datetime.datetime.now()
    delta = t1 - t0
    delta_s = delta.total_seconds()

    print ""
    print "Input EVT files = %i" % len(files)
    print "Parsed EVT files = %i" % files_ok
    print "EVT particles = %s" % evtcnt
    print "OPP particles = %s" % oppcnt
    print "OPP/EVT ratio = %.06f" % opp_evt_ratio
    print "Filtering completed in %.02f seconds" % delta_s


def ensure_opp_table(dbpath):
    """Ensure opp table exists."""
    con = sq.connect(dbpath)
    con.execute("""CREATE TABLE IF NOT EXISTS opp (
      -- First three columns are the EVT, OPP, VCT composite key
      cruise TEXT NOT NULL,
      file TEXT NOT NULL,  -- in old files, File+Day. in new files, Timestamp.
      particle INTEGER NOT NULL,
      -- Next we have the measurements. For these, see
      -- https://github.com/fribalet/flowPhyto/blob/master/R/Globals.R and look
      -- at version 3 of the evt header
      time INTEGER NOT NULL,
      pulse_width INTEGER NOT NULL,
      D1 REAL NOT NULL,
      D2 REAL NOT NULL,
      fsc_small REAL NOT NULL,
      fsc_perp REAL NOT NULL,
      fsc_big REAL NOT NULL,
      pe REAL NOT NULL,
      chl_small REAL NOT NULL,
      chl_big REAL NOT NULL,
      PRIMARY KEY (cruise, file, particle)
    )""")
    con.commit()
    con.close()


def ensure_opp_evt_ratio_table(dbpath):
    """Ensure opp_evt_ratio table exists."""
    con = sq.connect(dbpath)
    con.execute("""CREATE TABLE IF NOT EXISTS opp_evt_ratio (
      cruise TEXT NOT NULL,
      file TEXT NOT NULL,
      ratio REAL,
      PRIMARY KEY (cruise, file)
    )""")
    con.commit()
    con.close()


def create_indexes(dbpath):
    """Create opp table indexes."""
    t0 = datetime.datetime.now()

    print ""
    print "Creating opp table indexes"
    con = sq.connect(dbpath)
    index_cmds = [
        "CREATE INDEX IF NOT EXISTS oppFileIndex ON opp (file)",
        "CREATE INDEX IF NOT EXISTS oppFsc_smallIndex ON opp (fsc_small)",
        "CREATE INDEX IF NOT EXISTS oppPeIndex ON opp (pe)",
        "CREATE INDEX IF NOT EXISTS oppChl_smallIndex ON opp (chl_small)"
    ]
    for cmd in index_cmds:
        print cmd
        con.execute(cmd)
    con.commit()
    con.close()

    t1 = datetime.datetime.now()
    delta = t1 - t0
    delta_s = delta.total_seconds()
    print "Index creation completed in %.02f seconds" % delta_s


class EVT(object):
    """Class for EVT data operations"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.set_db_file_name()
        self.evtcnt = 0
        self.oppcnt = 0
        self.opp_evt_ratio = 0.0
        self.evt = None
        self.opp = None
        self.ok = False  # Could EVT file be parsed

        self.read_evt()

        # Set a flag to indicate if EVT file could be parsed
        if not self.evt is None:
            self.ok = True

    def set_db_file_name(self):
        """Set the file name to be used in the sqlite3 db."""
        pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}$'
        evt_re = re.compile(pattern)
        if evt_re.match(os.path.basename(self.file_path)):
            # New style EVT name
            self.db_file_name = os.path.basename(self.file_path)
        else:
            # Old style EVT name
            parts = self.file_path.split("/")
            if len(parts) < 2:
                raise ValueError(
                    "Old style EVT file paths must contain julian day directory")
            self.db_file_name = os.path.join(parts[-2], parts[-1])

    def read_evt(self):
        """Read an EVT binary file and return a pandas DataFrame."""
        cols = ["time", "pulse_width", "D1", "D2",
                "fsc_small", "fsc_perp", "fsc_big",
                "pe", "chl_small", "chl_big"]
        with open(self.file_path) as fh:
            try:
                rowcnt = np.fromfile(fh, dtype="uint32", count=1)
                particles = np.fromfile(fh, dtype="uint16", count=rowcnt*12)
                particles = np.reshape(particles, [rowcnt, 12])
                # Remove first 4 bytes of zero padding from each row
                self.evt = pd.DataFrame(np.delete(particles, [0, 1], 1),
                                        columns=cols)
                self.evt = self.evt.astype("float64")  # sqlite3 schema compat
                self.evtcnt = len(self.evt.index)
            except Exception:
                sys.stderr.write("Could not parse file %s\n" % self.file_path)

    def filter_particles(self, notch1=None, notch2=None, offset=None,
                         origin=None, width=None):
        """Filter EVT particle data."""
        if self.evt is None:
            return

        if (width is None) or (offset is None):
            raise ValueError(
                "Must supply width and offset to EVT.filter_particles")

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
            notch1 = max1 / (min1 + 10000)

        if notch2 is None:
            min2 = aligned[aligned["fsc_small"] == fsc_small_max]["D2"].min()
            max2 = aligned[aligned["D2"] == min2]["fsc_small"].max()
            notch2 = max2 / (min2 + 10000)

        # Filter focused particles (fsc_small > D + notch)
        oppD1 = aligned["fsc_small"] > ((aligned["D1"] * notch1) - (offset * 10**4))
        oppD2 = aligned["fsc_small"] > ((aligned["D2"] * notch2) - (offset * 10**4))
        opp = aligned[oppD1 & oppD2].copy()

        # Scale data (unsigned 16-bit numbers) to 3.5 decades
        discard = ["time", "pulse_width"]
        cols = [x for x in opp.columns if not x in discard]
        opp[cols] = 10**((opp[cols] / 2**16) * 3.5)

        self.opp = opp
        self.oppcnt = len(self.opp.index)
        try:
            self.opp_evt_ratio = float(self.oppcnt) / self.evtcnt
        except ZeroDivisionError:
            self.opp_evt_ratio = 0.0

    def add_extra_columns(self, cruise_name, particles_seen):
        """Add columns for cruise name, file name, and particle ID to OPP."""
        if self.opp is None:
            return

        ids = range(particles_seen, particles_seen + self.oppcnt)
        self.opp.insert(0, "cruise", cruise_name)
        self.opp.insert(1, "file", self.db_file_name)
        self.opp.insert(2, "particle", ids)

    def save_opp_to_db(self, cruise, oppcnt, dbpath):
        if self.opp is None:
            return

        self.add_extra_columns(cruise, oppcnt)
        self.insert_opp_particles_sqlite3(dbpath)
        self.insert_opp_evt_ratio_sqlite3(cruise, dbpath)

    def insert_opp_particles_sqlite3(self, dbpath):
        if self.opp is None:
            return

        sql = "INSERT INTO opp VALUES (%s)" % ",".join("?" * self.opp.shape[1])
        con = sq.connect(dbpath, timeout=30)
        cur = con.cursor()
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA cache_size=500000")
        cur.execute("PRAGMA journal_mode=memory")
        cur.executemany(sql, self.opp.itertuples(index=False))
        con.commit()

    def insert_opp_evt_ratio_sqlite3(self, cruise_name, dbpath):
        if self.opp is None:
            return

        sql = "INSERT INTO opp_evt_ratio VALUES (%s)" % ",".join("?"*3)
        con = sq.connect(dbpath, timeout=30)
        con.execute(sql, (cruise_name, self.db_file_name, self.opp_evt_ratio))
        con.commit()

    def write_opp_csv(self, outfile):
        if self.opp is None:
            return
        self.opp.to_csv(outfile, sep=",", index=False, header=False)

    def write_evt_csv(self, outfile):
        if self.evt is None:
            return
        self.evt.to_csv(outfile, sep=",", index=False)


if __name__ == "__main__":
    main()
