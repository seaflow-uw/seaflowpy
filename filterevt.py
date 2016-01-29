#!/usr/bin/env python

import argparse
from boto.s3.connection import S3Connection
import errno
import io
import getpass
import gzip
import multiprocessing as mp
import numpy as np
import pandas as pd
import pprint
import os
import random
import re
import shutil
import sqlite3
import sys
import time

# Global configuration variables for AWS
# ######################################
# Default name of Seaflow bucket
SEAFLOW_BUCKET = "seaflowdata"
# Default AWS region
AWS_REGION = "us-west-2"


def main():
    p = argparse.ArgumentParser(
        description="Filter EVT data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--files", nargs="+",
                   help="""EVT file paths. - to read from stdin.
                        (required unless --evt_dir or --s3)""")
    g.add_argument("--evt_dir",
                   help="EVT directory path (required unless --files or --s3)")
    g.add_argument("--s3", default=False, action="store_true",
                   help="""Read EVT files from s3://seaflowdata/CRUISE where
                        cruise is provided by --cruise (required unless --files
                        or --evt_dir)""")

    p.add_argument("--cpus", required=False, type=int, default=1,
                   help="""Number of CPU cores to use in filtering.
                        (optional)""")
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
    p.add_argument("--limit", type=int, default=None,
                   help="""Limit how many files to process. Useful for testing.
                        (optional)""")

    args = p.parse_args()

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
    elif args.evt_dir:
        files = find_evt_files(args.evt_dir)
    elif args.s3:
        # Make sure try to access S3 up front to setup AWS credentials before
        # launching child processes.
        files = get_s3_files(args.cruise)

    # Restrict length of file list with --limit
    if (not args.limit is None) and (args.limit > 0):
        files = files[:args.limit]

    # Filter
    filter_files(files, args.cpus, args.cruise, args.notch1, args.notch2,
                 args.width, args.origin, args.offset, args.progress,
                 args.s3, args.db)
    if not args.no_index:
        ensure_indexes(args.db)

# ----------------------------------------------------------------------------
# Functions and classes to manage filter workflows
# ----------------------------------------------------------------------------
def filter_files(files, cpus, cruise, notch1, notch2, width, origin, offset,
                 every, s3_flag, dbpath):
    t0 = time.time()

    print ""
    print "Filtering %i EVT files. Progress every %i%% (approximately)" % \
        (len(files), every)

    ensure_tables(dbpath)

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
            "s3": s3_flag,
            "dbpath": dbpath})

    last = 0  # Last progress milestone in increments of every
    evtcnt_block = 0  # EVT particles in this block (between milestones)
    oppcnt_block = 0  # OPP particles in this block

    # Filter particles in parallel with process pool
    for i, res in enumerate(pool.imap_unordered(do_work, inputs, 1)):
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


def do_work(params):
    """multiprocessing pool worker function"""
    try:
        return filter_one_file(params)
    except KeyboardInterrupt as e:
        pass


def filter_one_file(params):
    """Filter one EVT file, save to sqlite3, return filter stats"""
    result_keys = [
        "ok", "evtcnt", "oppcnt", "notch1", "notch2", "offset", "origin",
        "width", "path"]

    # Keys to pull from params for filter and save methods parameters
    filter_keys = ("notch1", "notch2", "offset", "origin", "width")
    save_keys = ("cruise", "dbpath")
    # Make methods parameter keyword dictionaries
    filter_kwargs = {k: params[k] for k in filter_keys}
    save_kwargs = {k: params[k] for k in save_keys}

    evt_file = params["file"]

    if params["s3"]:
        gzfile = download_s3_file_memory(params["file"])
        evt = EVT(path=evt_file, fileobj=gzfile)
    else:
        evt = EVT(path=evt_file)

    if evt.ok:
        evt.filter_particles(**filter_kwargs)
        evt.save_opp_to_db(**save_kwargs)

    result = { k: getattr(evt, k) for k in result_keys }
    return result


class EVT(object):
    """Class for EVT data operations"""

    # EVT file name regexes. Does not contain directory names.
    new_re = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}(\.gz)?$')
    old_re = re.compile(r'^\d+\.evt(\.gz)?$')

    @staticmethod
    def is_evt(path):
        """Does the file specified by this path look like an EVT file?"""
        basename = os.path.basename(path)  # Don't check directories
        return bool(EVT.new_re.match(basename) or EVT.old_re.match(basename))

    def __init__(self, path=None, fileobj=None):
        # If fileobj is set, read data from this object. The path will be used
        # to set the file name in the database and detect compression.
        self.path = path  # EVT file path, local or in S3
        self.fileobj = fileobj  # EVT data in file object

        self.evtcnt = 0
        self.oppcnt = 0
        self.opp_evt_ratio = 0.0
        self.evt = None
        self.opp = None
        self.ok = False  # Could EVT file be parsed

        # Set filter params to None
        # Should be set in filter_particles()
        self.notch1 = None
        self.notch2 = None
        self.offset = None
        self.origin = None
        self.width = None

        try:
            self.read_evt()
        except Exception as e:
            print "Could not parse file %s: %s" % (self.path, repr(e))

        # Set a flag to indicate if EVT file could be parsed
        if not self.evt is None:
            self.ok = True

    def isgz(self):
        return self.path and self.path.endswith(".gz")

    def get_db_file_name(self):
        """Get the file name to be used in the sqlite3 db."""
        db_file_name = None

        if self.isgz():
            path = self.path[:-3]  # remove .gz
        else:
            path = self.path

        if self.new_re.match(os.path.basename(path)):
            # New style EVT name
            db_file_name = os.path.basename(path)
        elif self.old_re.match(os.path.basename(path)):
            # Old style EVT name
            parts = path.split("/")
            if len(parts) < 2:
                raise EVTFileError(
                    "Old style EVT file paths must contain julian day directory")
            db_file_name = os.path.join(parts[-2], parts[-1])
        else:
            raise EVTFileError("File name does not look like an EVT file: %s" %
                os.path.basename(path))

        return db_file_name

    def open(self):
        """Return a EVT file-like object for reading."""
        handle = None
        if self.fileobj:
            if self.isgz():
                handle = gzip.GzipFile(fileobj=self.fileobj)
            else:
                handle = self.fileobj
        else:
            if self.isgz():
                handle = gzip.GzipFile(self.path, "r")
            else:
                handle = open(self.path, "r")
        return handle

    def read_evt(self):
        """Read an EVT binary file and return a pandas DataFrame."""
        # Data columns
        cols = ["time", "pulse_width", "D1", "D2",
                "fsc_small", "fsc_perp", "fsc_big",
                "pe", "chl_small", "chl_big"]

        with self.open() as fh:
            # Particle count (rows of data) is stored in an initial 32-bit
            # unsigned int
            buff = fh.read(4)
            if len(buff) == 0:
                raise EVTFileError("File is empty")
            if len(buff) != 4:
                raise EVTFileError("File has invalid particle count header")
            rowcnt = np.fromstring(buff, dtype="uint32", count=1)[0]
            # Read the rest of the data. Each particle has 12 unsigned
            # 16-bit ints in a row.
            expected_bytes = rowcnt * 12 * 2  # rowcnt * 12 columns * 2 bytes
            buff = fh.read(expected_bytes)
            if len(buff) != expected_bytes:
                raise EVTFileError(
                    "File has incorrect number of data bytes. Expected %i, saw %i" %
                    (expected_bytes, len(buff)))
            particles = np.fromstring(buff, dtype="uint16", count=rowcnt*12)
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
        ignore = ["time", "pulse_width"]
        cols = [x for x in opp.columns if not x in ignore]
        opp[cols] = 10**((opp[cols] / 2**16) * 3.5)

        self.opp = opp
        self.oppcnt = len(self.opp.index)
        try:
            self.opp_evt_ratio = float(self.oppcnt) / self.evtcnt
        except ZeroDivisionError:
            self.opp_evt_ratio = 0.0

        self.notch1 = notch1
        self.notch2 = notch2
        self.offset = offset
        self.origin = origin
        self.width = width

    def add_extra_columns(self, cruise_name):
        """Add columns for cruise name, file name, and particle ID to OPP."""
        if self.opp is None:
            return

        self.opp.insert(0, "cruise", cruise_name)
        self.opp.insert(1, "file", self.get_db_file_name())
        self.opp.insert(2, "particle", range(1, self.oppcnt+1))

    def save_opp_to_db(self, cruise, dbpath):
        if self.opp is None:
            return

        self.add_extra_columns(cruise)
        self.insert_opp_particles_sqlite3(dbpath)
        self.insert_filter_sqlite3(cruise, dbpath)

    def insert_opp_particles_sqlite3(self, dbpath):
        if self.opp is None:
            return

        sql = "INSERT INTO opp VALUES (%s)" % ",".join("?" * self.opp.shape[1])
        con = sqlite3.connect(dbpath, timeout=30)
        cur = con.cursor()
        cur.executemany(sql, self.opp.itertuples(index=False))
        con.commit()

    def insert_filter_sqlite3(self, cruise_name, dbpath):
        if self.opp is None:
            return

        # cruise, file, evt_count, opp_count, opp_evt_ratio, notch1, notch2,
        # offset, origin, width
        sql = "INSERT INTO filter VALUES (%s)" % ",".join("?"*10)
        con = sqlite3.connect(dbpath, timeout=30)
        con.execute(
            sql,
            (cruise_name, self.get_db_file_name(), self.oppcnt, self.evtcnt,
                self.opp_evt_ratio, self.notch1, self.notch2, self.offset,
                self.origin, self.width))
        con.commit()

    def write_opp_csv(self, outfile):
        if self.opp is None:
            return
        self.opp.to_csv(outfile, sep=",", index=False, header=False)

    def write_evt_csv(self, outfile):
        if self.evt is None:
            return
        self.evt.to_csv(outfile, sep=",", index=False)


class EVTFileError(Exception):
    """Custom exception class for EVT file format errors"""
    pass


# ----------------------------------------------------------------------------
# Functions to manage lists of local EVT files
# ----------------------------------------------------------------------------
def parse_file_list(files):
    files_list = []
    if len(files) and files[0] == "-":
        for line in sys.stdin:
            if EVT.is_evt(f):
                files_list.append(line.rstrip())
    else:
        for f in files:
            if EVT.is_evt(f):
                files_list.append(f)
    return files_list


def find_evt_files(evt_dir):
    evt_files = []

    for root, dirs, files in os.walk(evt_dir):
        for f in files:
            if EVT.is_evt(f):
                evt_files.append(os.path.join(root, f))

    return evt_files


# ----------------------------------------------------------------------------
# AWS functions
# ----------------------------------------------------------------------------
def get_aws_credentials():
    aws_access_key_id = getpass.getpass("aws_access_key_id: ")
    aws_secret_access_key = getpass.getpass("aws_secret_access_key: ")
    return (aws_access_key_id, aws_secret_access_key)


def save_aws_credentials(aws_access_key_id, aws_secret_access_key):
    # Make ~/.aws config directory
    awsdir = os.path.join(os.environ["HOME"], ".aws")
    mkdir_p(awsdir)

    flags = os.O_WRONLY | os.O_CREAT

    # Make credentials file
    credentials = os.path.join(awsdir, "credentials")
    with os.fdopen(os.open(credentials, flags, 0600), "w") as fh:
        fh.write("[default]\n")
        fh.write("aws_access_key_id = %s\n" % aws_access_key_id)
        fh.write("aws_secret_access_key = %s\n" % aws_secret_access_key)

    # May as well make config file and set default region while we're at it
    config = os.path.join(awsdir, "config")
    with os.fdopen(os.open(config, flags, 0600), "w") as fh:
        fh.write("[default]\n")
        fh.write("region = %s\n" % AWS_REGION)


def get_s3_connection():
    try:
        s3 = S3Connection()
    except:
        (aws_access_key_id, aws_secret_access_key) = get_aws_credentials()
        s3 = S3Connection(aws_access_key_id, aws_secret_access_key)
        # Save credentials so we don't have to do this all the time
        # And so that any child processes have acces to AWS resources
        save_aws_credentials(aws_access_key_id, aws_secret_access_key)
    return s3


def get_s3_files(cruise):
    s3 = get_s3_connection()
    bucket = s3.get_bucket(SEAFLOW_BUCKET, validate=True)
    i = 0
    files = []
    for item in bucket.list(prefix=cruise + "/"):
        # Only keep files for this cruise and skip SFL files
        if str(item.key) != "%s/" % cruise:
            # Make sure this looks like an EVT file
            if EVT.is_evt(str(item.key)):
                files.append(str(item.key))
    return files


def download_s3_file_memory(key_str, retries=5):
    """Return S3 file contents in io.BytesIO file-like object"""
    tries = 0
    while True:
        try:
            s3 = get_s3_connection()
            bucket = s3.get_bucket("seaflowdata", validate=True)
            key = bucket.get_key(key_str)
            data = io.BytesIO(key.get_contents_as_string())
            return data
        except:
            tries += 1
            if tries == retries:
                raise
            sleep = (2**(tries-1)) + random.random()
            time.sleep(sleep)


# ----------------------------------------------------------------------------
# Database functions
# ----------------------------------------------------------------------------
def ensure_tables(dbpath):
    """Ensure all popcycle tables exists."""
    con = sqlite3.connect(dbpath)
    cur = con.cursor()

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

    cur.execute("""CREATE TABLE IF NOT EXISTS vct (
        -- First three columns are the EVT, OPP, VCT, SDS composite key
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,  -- in old files, File+Day. in new files, Timestamp.
        particle INTEGER NOT NULL,
        -- Next we have the classification
        pop TEXT NOT NULL,
        method TEXT NOT NULL,
        PRIMARY KEY (cruise, file, particle)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS filter (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
        opp_count INTEGER NOT NULL,
        evt_count INTEGER NOT NULL,
        opp_evt_ratio REAL NOT NULL,
        notch1 REAL NOT NULL,
        notch2 REAL NOT NULL,
        offset REAL NOT NULL,
        origin REAL NOT NULL,
        width REAL NOT NULL,
        PRIMARY KEY (cruise, file)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS sfl (
        --First two columns are the SFL composite key
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,  -- in old files, File+Day. in new files, Timestamp.
        date TEXT,
        file_duration REAL,
        lat REAL,
        lon REAL,
        conductivity REAL,
        salinity REAL,
        ocean_tmp REAL,
        par REAL,
        bulk_red REAL,
        stream_pressure REAL,
        flow_rate REAL,
        event_rate REAL,
        PRIMARY KEY (cruise, file)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS stats (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
        time TEXT,
        lat REAL,
        lon REAL,
        opp_evt_ratio REAL,
        flow_rate REAL,
        file_duration REAL,
        pop TEXT NOT NULL,
        n_count INTEGER,
        abundance REAL,
        fsc_small REAL,
        chl_small REAL,
        pe REAL,
        PRIMARY KEY (cruise, file, pop)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS cytdiv (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
        N0 INTEGER,
        N1 REAL,
        H REAL,
        J REAL,
        opp_red REAL,
        PRIMARY KEY (cruise, file)
    )""")

    con.commit()
    con.close()


def ensure_indexes(dbpath):
    """Create opp table indexes."""
    t0 = time.time()

    print ""
    print "Creating opp table indexes"
    con = sqlite3.connect(dbpath)
    cur = con.cursor()
    index_cmds = [
        "CREATE INDEX IF NOT EXISTS oppFileIndex ON opp (file)",
        "CREATE INDEX IF NOT EXISTS oppFsc_smallIndex ON opp (fsc_small)",
        "CREATE INDEX IF NOT EXISTS oppPeIndex ON opp (pe)",
        "CREATE INDEX IF NOT EXISTS oppChl_smallIndex ON opp (chl_small)",
        "CREATE INDEX IF NOT EXISTS vctFileIndex ON vct (file)",
        "CREATE INDEX IF NOT EXISTS sflDateIndex ON sfl (date)"
    ]
    for cmd in index_cmds:
        print cmd
        cur.execute(cmd)
    con.commit()
    con.close()

    t1 = time.time()
    print "Index creation completed in %.2f seconds" % (t1 - t0,)


if __name__ == "__main__":
    main()
