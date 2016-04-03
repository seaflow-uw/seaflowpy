import aws
import copy
import db
import evt
import errors
import os
import time
import util
from itertools import imap
from multiprocessing import Pool


def filter_evt_files(**kwargs):
    """Filter a list of EVT files.

    Keyword arguments:
        files - paths to EVT files to filter
        cruise - cruise name
        cpus - number of worker processes to use
        filter_options - Dictionary of filter params
            (notch1, notch2, width, offset, origin)
        every - Percent progress output resolution
        s3 - Get EVT data from S3
        s3_bucket - S3 bucket name
        db = SQLite3 db path
        opp_dir = Directory for output binary OPP files
        multiprocessing = Use multiprocessing?
    """
    o = {
        "files": [],
        "cruise": None,
        "cpus": 1,
        "filter_options": {},
        "every": 10.0,
        "s3": False,
        "s3_bucket": None,
        "db": None,
        "opp_dir": None,
        "multiprocessing": True
    }
    o.update(kwargs)

    if not o["filter_options"]:
        raise ValueError("Must specify keyword arg filter_options in filter_files()")

    if o["db"]:
        dbdir = os.path.dirname(o["db"])
        if dbdir and not os.path.isdir(dbdir):
            util.mkdir_p(dbdir)
        db.ensure_tables(o["db"])
        o["filter_id"] = db.save_filter_params(o["db"], o["filter_options"])

    if o["multiprocessing"]:
        # Create a pool of N worker processes
        pool = Pool(o["cpus"])
        def mapper(worker, task_list):
            return pool.imap_unordered(worker, task_list)
    else:
        def mapper(worker, task_list):
            return imap(worker, task_list)

    evt_count = 0
    opp_count = 0
    files_ok = 0

    # Construct worker inputs
    inputs = []
    files = o.pop("files")
    for f in files:
        inputs.append(copy.copy(o))
        inputs[-1]["file"] = f

    print ""
    print "Filtering %i EVT files. Progress every %i%% (approximately)" % \
        (len(files), o["every"])

    t0 = time.time()

    last = 0  # Last progress milestone in increments of every
    evt_count_block = 0  # EVT particles in this block (between milestones)
    opp_count_block = 0  # OPP particles in this block

    # Filter particles in parallel with process pool
    for i, res in enumerate(mapper(do_work, inputs)):
        evt_count_block += res["evt_count"]
        opp_count_block += res["opp_count"]
        files_ok += 1 if res["ok"] else 0

        # Print progress periodically
        perc = float(i + 1) / len(files) * 100  # Percent completed
        # Round down to closest every%
        milestone = int(perc / o["every"]) * o["every"]
        if milestone > last:
            now = time.time()
            evt_count += evt_count_block
            opp_count += opp_count_block
            try:
                ratio_block = float(opp_count_block) / evt_count_block
            except ZeroDivisionError:
                ratio_block = 0.0
            msg = "File: %i/%i (%.02f%%)" % (i + 1, len(files), perc)
            msg += " Particles this block: %i / %i (%.06f) elapsed: %.2fs" % \
                (opp_count_block, evt_count_block, ratio_block, now - t0)
            print msg
            last = milestone
            evt_count_block = 0
            opp_count_block = 0
    # If any particle count data is left, add it to totals
    if evt_count_block > 0:
        evt_count += evt_count_block
        opp_count += opp_count_block

    try:
        opp_evt_ratio = float(opp_count) / evt_count
    except ZeroDivisionError:
        opp_evt_ratio = 0.0

    t1 = time.time()
    delta = t1 - t0
    try:
        evtrate = float(evt_count) / delta
    except ZeroDivisionError:
        evtrate = 0.0
    try:
        opprate = float(opp_count) / delta
    except ZeroDivisionError:
        opprate = 0.0

    print ""
    print "Input EVT files = %i" % len(files)
    print "Parsed EVT files = %i" % files_ok
    print "EVT particles = %s (%.2f p/s)" % (evt_count, evtrate)
    print "OPP particles = %s (%.2f p/s)" % (opp_count, opprate)
    print "OPP/EVT ratio = %.06f" % opp_evt_ratio
    print "Filtering completed in %.2f seconds" % (delta,)


def do_work(options):
    """multiprocessing pool worker function"""
    try:
        return filter_one_file(**options)
    except KeyboardInterrupt as e:
        pass


def filter_one_file(**kwargs):
    """Filter one EVT file, save to sqlite3, return filter stats"""
    o = kwargs
    result = {
        "ok": False,
        "evt_count": 0,
        "opp_count": 0,
        "path": o["file"]
    }

    evt_file = o["file"]
    fileobj = None
    if o["s3"]:
        fileobj = aws.download_s3_file_memory(evt_file, o["s3_bucket"])

    try:
        evt_ = evt.EVT(path=evt_file, fileobj=fileobj)
    except errors.EVTFileError as e:
        print "Could not parse file %s: %s" % (evt_file, repr(e))
    except:
        print "Unexpected error for file %s" % evt_file
    else:
        evt_.filter(**o["filter_options"])

        if o["db"]:
            evt_.save_opp_to_db(o["cruise"], o["filter_id"], o["db"])

        if o["opp_dir"]:
            # Might have julian day, might not
            outdir = os.path.join(
                o["opp_dir"],
                os.path.dirname(evt_.get_julian_path())
            )
            util.mkdir_p(outdir)
            outfile = os.path.join(
                o["opp_dir"],
                evt_.get_julian_path() + ".opp.gz"
            )
            evt_.write_opp_binary(outfile)

        result["ok"] = True
        result["evt_count"] = evt_.evt_count
        result["opp_count"] = evt_.opp_count

    return result
