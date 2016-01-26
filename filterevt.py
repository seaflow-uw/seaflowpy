#!/usr/bin/env python

import argparse
import time
import multiprocessing as mp
import numpy as np
import pandas as pd
import pprint
import os
import re
import sqlite3 as sq
import sys


def main():
    p = argparse.ArgumentParser(
        description="Filter EVT data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--files", nargs="+",
                   help="""EVT file paths. - to read from stdin.
                        (required unless --evt_dir)""")
    g.add_argument("--evt_dir",
                   help="EVT directory path (required unless --files)")

    p.add_argument("--cpus", required=False, type=int, default=1,
                   help="""Number of CPU cores to use in filtering. Max value of
                   system core count. (optional)""")
    p.add_argument("--db", required=True, help="sqlite3 db file (required)")
    p.add_argument("--cruise", required=True, help="cruise name (required)")
    p.add_argument("--notch1", type=float, help="notch 1 (optional)")
    p.add_argument("--notch2", type=float, help="notch 2 (optional)")
    p.add_argument("--width", type=float, default=0.5, help="width (optional)")
    p.add_argument("--origin", type=float, help="origin (optional)")
    p.add_argument("--offset", type=float, default=0.0,
                   help="offset (optional)")
    p.add_argument("--no_index", default=False, action="store_true",
                   help="Skip creation of opp table indexes (optional)")
    p.add_argument("--progress", type=float, default=10.0,
                   help="Progress update %% resolution (optional)")

    args = p.parse_args()

    # Check --cpus option
    args.cpus = min(mp.cpu_count(), args.cpus)

    # Print defined parameters
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    print "Defined parameters:"
    pprint.pprint(v, indent=2)

    # Find EVT files
    if args.files:
        files = parse_file_list(args.files)
    else:
        files = find_evt_files(args.evt_dir)

    # Filter
    filter_files(files, args.cpus, args.cruise, args.notch1, args.notch2,
                 args.width, args.origin, args.offset, args.progress, args.db)
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


def filter_files(files, cpus, cruise, notch1, notch2, width, origin, offset,
                 every, dbpath):
    t0 = time.time()

    print ""
    print "Filtering %i EVT files. Progress every %i%% (approximately)" % \
        (len(files), every)

    ensure_opp_table(dbpath)
    ensure_opp_evt_ratio_table(dbpath)

    evtcnt = 0
    oppcnt = 0
    files_ok = 0

    # Create a pool of N worker processes
    pool = mp.Pool(cpus)

    # Construct worker inputs
    inputs = []
    for f in files:
        inputs.append({
            "file": f,
            "cruise": cruise,
            "notch1": notch1,
            "notch2": notch2,
            "width": width,
            "origin": origin,
            "offset": offset,
            "dbpath": dbpath})

    last = 0  # Last progress milestone in increments of every
    evtcnt_block = 0  # EVT particles in this block (between milestones)
    oppcnt_block = 0  # OPP particles in this block

    # Filter particles in parallel with process pool
    for i, res in enumerate(pool.imap_unordered(filter_one_file, inputs, 1)):
        evtcnt_block += res["evtcnt"]
        oppcnt_block += res["oppcnt"]
        files_ok += 1 if res["ok"] else 0

        # Print progress periodically
        perc = float(i + 1) / len(files) * 100  # Percent completed
        milestone = int(perc / every) * every   # Round down to closest every%
        if milestone > last:
            now = time.time()
            evtcnt += evtcnt_block
            oppcnt += oppcnt_block
            try:
                ratio_block = float(oppcnt_block) / evtcnt_block
            except ZeroDivisionError:
                ratio_block = 0.0
            msg = "File: %i/%i (%.02f%%)" % (i + 1, len(files), perc)
            msg += " Particles this block: %i / %i (%.06f) elapsed: %.2fs" % \
                (oppcnt_block, evtcnt_block, ratio_block, now - t0)
            print msg
            last = milestone
            evtcnt_block = 0
            oppcnt_block = 0
    # If any particle count data is left, add it to totals
    if evtcnt_block > 0:
        evtcnt += evtcnt_block
        oppcnt += oppcnt_block

    try:
        opp_evt_ratio = float(oppcnt) / evtcnt
    except ZeroDivisionError:
        opp_evt_ratio = 0.0

    t1 = time.time()

    print ""
    print "Input EVT files = %i" % len(files)
    print "Parsed EVT files = %i" % files_ok
    print "EVT particles = %s" % evtcnt
    print "OPP particles = %s" % oppcnt
    print "OPP/EVT ratio = %.06f" % opp_evt_ratio
    print "Filtering completed in %.2f seconds" % (t1 - t0,)


def filter_one_file(params):
    # Keys to pull from params for filter and save methods parameters
    filter_keys = ("notch1", "notch2", "offset", "origin", "width")
    save_keys = ("cruise", "dbpath")
    # Make methods parameter keyword dictionaries
    filter_kwargs = {k: params[k] for k in filter_keys}
    save_kwargs = {k: params[k] for k in save_keys}

    evt = EVT(params["file"])
    if evt.ok:
        evt.filter_particles(**filter_kwargs)
        evt.save_opp_to_db(**save_kwargs)

    return {"ok": evt.ok, "evtcnt": evt.evtcnt, "oppcnt": evt.oppcnt}


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
    t0 = time.time()

    print ""
    print "Creating opp table indexes"
    con = sq.connect(dbpath)
    cur = con.cursor()
    index_cmds = [
        "CREATE INDEX IF NOT EXISTS oppFileIndex ON opp (file)",
        "CREATE INDEX IF NOT EXISTS oppFsc_smallIndex ON opp (fsc_small)",
        "CREATE INDEX IF NOT EXISTS oppPeIndex ON opp (pe)",
        "CREATE INDEX IF NOT EXISTS oppChl_smallIndex ON opp (chl_small)"
    ]
    for cmd in index_cmds:
        print cmd
        cur.execute(cmd)
    con.commit()
    con.close()

    t1 = time.time()
    print "Index creation completed in %.2f seconds" % (t1 - t0,)


class EVT(object):
    """Class for EVT data operations"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.set_file_name()
        self.evtcnt = 0
        self.oppcnt = 0
        self.opp_evt_ratio = 0.0
        self.evt = None
        self.opp = None
        self.ok = False  # Could EVT file be parsed

        try:
            self.read_evt()
        except Exception as e:
            print "Could not parse file %s: %s" % (self.file_path, repr(e))

        # Set a flag to indicate if EVT file could be parsed
        if not self.evt is None:
            self.ok = True

    def set_file_name(self):
        """Set the file name to be used in the sqlite3 db."""
        pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}$'
        evt_re = re.compile(pattern)
        if evt_re.match(os.path.basename(self.file_path)):
            # New style EVT name
            self.file_name = os.path.basename(self.file_path)
        else:
            # Old style EVT name
            parts = self.file_path.split("/")
            if len(parts) < 2:
                raise ValueError(
                    "Old style EVT file paths must contain julian day directory")
            self.file_name = os.path.join(parts[-2], parts[-1])

    def read_evt(self):
        """Read an EVT binary file and return a pandas DataFrame."""
        # Data columns
        cols = ["time", "pulse_width", "D1", "D2",
                "fsc_small", "fsc_perp", "fsc_big",
                "pe", "chl_small", "chl_big"]

        # Check for empty file
        file_size = os.stat(self.file_path).st_size
        if file_size == 0:
            raise Exception("File is empty")

        with open(self.file_path) as fh:
            # Particle count (rows of data) is stored in an initial 32-bit
            # unsigned int
            rowcnt = np.fromfile(fh, dtype="uint32", count=1)
            # Make sure the file is the expected size based on particle count
            expected_size = 4 + (rowcnt * (2 * 12))
            if file_size != expected_size:
                raise Exception(
                    "Incorrect file size. Expected %i, saw %i." % (expected_size,
                                                                   file_size))
            # Read the rest of the data. Each particle has 12 unsigned
            # 16-bit ints in a row.
            particles = np.fromfile(fh, dtype="uint16", count=rowcnt*12)
            # Reshape into a matrix of 12 columns and one row per particle
            particles = np.reshape(particles, [rowcnt, 12])
            # Create a Pandas DataFrame. The first two zeroed uint16s from
            # start of each row are left out. These empty ints are an
            # idiosyncrasy of LabVIEW's binary output format. Label each
            # column with a descriptive name.
            self.evt = pd.DataFrame(np.delete(particles, [0, 1], 1),
                                    columns=cols)
            # Cast as 64-bit floats. Could do 32-bit except SQLite 3 only
            # has 64-bit floats.
            self.evt = self.evt.astype("float64")

            # Record the original number of particles
            self.evtcnt = len(self.evt.index)

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

    def add_extra_columns(self, cruise_name):
        """Add columns for cruise name, file name, and particle ID to OPP."""
        if self.opp is None:
            return

        self.opp.insert(0, "cruise", cruise_name)
        self.opp.insert(1, "file", self.file_name)
        self.opp.insert(2, "particle", range(1, self.oppcnt+1))

    def save_opp_to_db(self, cruise, dbpath):
        if self.opp is None:
            return

        self.add_extra_columns(cruise)
        self.insert_opp_particles_sqlite3(dbpath)
        self.insert_opp_evt_ratio_sqlite3(cruise, dbpath)

    def insert_opp_particles_sqlite3(self, dbpath):
        if self.opp is None:
            return

        sql = "INSERT INTO opp VALUES (%s)" % ",".join("?" * self.opp.shape[1])
        con = sq.connect(dbpath, timeout=30)
        cur = con.cursor()
        cur.executemany(sql, self.opp.itertuples(index=False))
        con.commit()

    def insert_opp_evt_ratio_sqlite3(self, cruise_name, dbpath):
        if self.opp is None:
            return

        sql = "INSERT INTO opp_evt_ratio VALUES (%s)" % ",".join("?"*3)
        con = sq.connect(dbpath, timeout=30)
        con.execute(sql, (cruise_name, self.file_name, self.opp_evt_ratio))
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
