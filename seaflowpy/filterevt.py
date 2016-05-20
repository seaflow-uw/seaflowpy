import aws
import copy
import db
import evt
import errors
import os
import sys
import time
import util
from itertools import imap
from multiprocessing import Pool


def filter_evt_files(files, cruise, filter_options, dbpath, opp_dir, s3=False,
                     s3_bucket=None, process_count=1, every=10.0,
                     multiprocessing_flag=True):
    """Filter a list of EVT files.

    Arguments arguments:
        files - paths to EVT files to filter
        cruise - cruise name
        filter_options - Dictionary of filter params
            (notch1, notch2, width, offset, origin)
        dbpath = SQLite3 db path
        opp_dir = Directory for output binary OPP files

    Keyword arguments:
        s3 - Get EVT data from S3
        s3_bucket - S3 bucket name
        process_count - number of worker processes to use
        every - Percent progress output resolution
        multiprocessing_flag = Use multiprocessing?
    """
    o = {
        "file": None,  # fill in later
        "cruise": cruise,
        "process_count": process_count,
        "filter_options": filter_options,
        "every": every,
        "s3": s3,
        "s3_bucket": s3_bucket,
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "multiprocessing_flag": multiprocessing_flag,
        "filter_id": None  # fill in later
    }

    if dbpath:
        dbdir = os.path.dirname(dbpath)
        if dbdir and not os.path.isdir(dbdir):
            util.mkdir_p(dbdir)
        db.ensure_tables(dbpath)
        o["filter_id"] = db.save_filter_params(dbpath, filter_options)

    if multiprocessing_flag:
        # Create a pool of N worker processes
        pool = Pool(process_count)
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
    for f in files:
        inputs.append(copy.copy(o))
        inputs[-1]["file"] = f

    print ""
    print "Filtering %i EVT files. Progress every %i%% (approximately)" % \
        (len(files), every)

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
        milestone = int(perc / every) * every
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
            sys.stdout.flush()
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
        return filter_one_file(options)
    except KeyboardInterrupt as e:
        pass


def filter_one_file(o):
    """Filter one EVT file, save to sqlite3, return filter stats"""
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
    except Exception as e:
        print "Unexpected error for file %s: %s" % (evt_file, repr(e))

    else:
        evt_.filter(**o["filter_options"])

        if o["dbpath"]:
            evt_.save_opp_to_db(o["cruise"], o["filter_id"], o["dbpath"])

        if o["opp_dir"]:
            evt_.write_opp_binary(o["opp_dir"])

        result["ok"] = True
        result["evt_count"] = evt_.evt_count
        result["opp_count"] = evt_.opp_count

    return result
