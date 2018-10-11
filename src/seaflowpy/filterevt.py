from __future__ import division
from __future__ import print_function
from builtins import map
from . import clouds
from . import conf
import copy
from . import db
from . import evt
from . import errors
import json
import os
import sys
import time

from multiprocessing import Pool


def filter_evt_files(files, dbpath, opp_dir, s3=False, process_count=1,
                     every=10.0):
    """Filter a list of EVT files.

    Positional arguments:
        files - paths to EVT files to filter
        dbpath = SQLite3 db path
        opp_dir = Directory for output binary OPP files

    Keyword arguments:
        s3 - Get EVT data from S3
        process_count - number of worker processes to use
        every - Percent progress output resolution
    """
    o = {
        "file": None,  # fill in later
        "process_count": process_count,
        "every": every,
        "s3": s3,
        "cloud_config_items": None,
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "filter_params": None  # fill in later from db
    }

    if not dbpath:
        raise ValueError("Must provide db path to filter_evt_files()")

    filter_df = db.get_latest_filter(dbpath)

    # Turn pandas dataframe into dictionary keyed by quantile for convenience
    o["filter_params"] = {}
    for q in [2.5, 50, 97.5]:
        o["filter_params"][q] = dict(filter_df[filter_df["quantile"] == q].iloc[0])
    o["filter_id"] = o["filter_params"][2.5]["id"]

    if s3:
        config = conf.get_aws_config(s3_only=True)
        o["cloud_config_items"] = config.items("aws")

    if process_count > 1:
        # Create a pool of N worker processes
        pool = Pool(process_count)
        def mapper(worker, task_list):
            return pool.imap_unordered(worker, task_list)
    else:
        def mapper(worker, task_list):
            return map(worker, task_list)

    evt_count = 0
    evt_signal_count = 0
    opp_count = 0
    files_ok = 0

    # Construct worker inputs
    inputs = []
    for f in files:
        inputs.append(copy.copy(o))
        inputs[-1]["file"] = f

    print("")
    print("Filtering %i EVT files. Progress every %i%% (approximately)" % \
        (len(files), every))

    t0 = time.time()

    last = 0  # Last progress milestone in increments of every
    evt_count_block = 0  # EVT particles in this block (between milestones)
    evt_signal_count_block = 0  # EVT noise filtered particles in this block
    opp_count_block = 0  # OPP particles in this block

    # Filter particles in parallel with process pool
    for i, res in enumerate(mapper(do_work, inputs)):
        evt_count_block += res["evt_count"]
        evt_signal_count_block += res["evt_signal_count"]
        opp_count_block += res["opp_count"]
        files_ok += 1 if res["ok"] else 0

        # Print progress periodically
        perc = float(i + 1) / len(files) * 100  # Percent completed
        # Round down to closest every%
        milestone = int(perc / every) * every
        if milestone > last:
            now = time.time()
            evt_count += evt_count_block
            evt_signal_count += evt_signal_count_block
            opp_count += opp_count_block
            ratio_signal_block = zerodiv(opp_count_block, evt_signal_count_block)
            ratio_block = zerodiv(opp_count_block, evt_count_block)
            msg = "File: %i/%i (%.02f%%)" % (i + 1, len(files), perc)
            msg += " Particles this block: %i / %i (%i) %.04f (%.04f) elapsed: %.2fs" % \
                (opp_count_block, evt_signal_count_block, evt_count_block,
                ratio_signal_block, ratio_block, now - t0)
            print(msg)
            sys.stdout.flush()
            last = milestone
            evt_count_block = 0
            evt_signal_count_block = 0
            opp_count_block = 0
    # If any particle count data is left, add it to totals
    if evt_count_block > 0:
        evt_count += evt_count_block
        evt_signal_count += evt_signal_count_block
        opp_count += opp_count_block

    opp_evt_signal_ratio = zerodiv(opp_count, evt_signal_count)
    opp_evt_ratio = zerodiv(opp_count, evt_count)

    t1 = time.time()
    delta = t1 - t0
    evtrate = zerodiv(evt_count, delta)
    evtsignalrate = zerodiv(evt_signal_count, delta)
    opprate = zerodiv(opp_count, delta)

    print("")
    print("Input EVT files = %i" % len(files))
    print("Parsed EVT files = %i" % files_ok)
    print("EVT particles = %s (%.2f p/s)" % (evt_count, evtrate))
    print("EVT noise filtered particles = %s (%.2f p/s)" % (evt_signal_count, evtsignalrate))
    print("OPP particles = %s (%.2f p/s)" % (opp_count, opprate))
    print("OPP/EVT ratio = %.04f (%.04f)" % (opp_evt_signal_ratio, opp_evt_ratio))
    print("Filtering completed in %.2f seconds" % (delta,))


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
        "evt_signal_count": 0,
        "opp_count": 0,
        "path": o["file"]
    }

    evt_file = o["file"]
    fileobj = None
    if o["s3"]:
        cloud = clouds.AWS(o["cloud_config_items"])
        fileobj = cloud.download_file_memory(evt_file)

    # Try to read EVT file data, if an error occurs create an EVT object without
    # any data.
    try:
        evt_ = evt.EVT(path=evt_file, fileobj=fileobj)
    except errors.FileError as e:
        print("Could not parse file %s: %s" % (evt_file, repr(e)))
        evt_ = evt.EVT(path=evt_file, fileobj=fileobj, read_data=False)
    except Exception as e:
        print("Unexpected error for file %s: %s" % (evt_file, repr(e)))
        evt_ = evt.EVT(path=evt_file, fileobj=fileobj, read_data=False)

    qs = {}
    for q in [2.5, 50, 97.5]:
        opp = evt_.filter(o["filter_params"][q])
        qs[q] = opp
    if o["dbpath"]:
        for q in qs:
            opp = qs[q]
            opp.save_opp_to_db(o["filter_id"], q, o["dbpath"])

    all_quantiles_have_opp = min([opp.particle_count for opp in qs.values()]) > 0
    if o["opp_dir"] and all_quantiles_have_opp:
        # Only write files if all quantiles produced OPP data
        for q in qs:
            opp = qs[q]
            opp.write_binary(o["opp_dir"], opp=True, quantile=q)

    # Only report 50 quantile data
    opp = qs[50]
    result["ok"] = True
    result["evt_count"] = opp.parent.event_count
    result["evt_signal_count"] = opp.parent.particle_count
    result["opp_count"] = opp.particle_count

    return result


def zerodiv(x, y):
    """Divide x by y, floating point, and default to 0.0 if divisor is 0"""
    try:
        answer = float(x) / float(y)
    except ZeroDivisionError:
        answer = 0.0
    return answer
