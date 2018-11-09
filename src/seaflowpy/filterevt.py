from . import clouds
from .conf import get_aws_config
from . import db
from . import evt
from . import errors
from . import util
import copy
import json
import os
import sys
import time
from multiprocessing import Process, Queue

# Stop sentinel value for queues
stop = 'STOP'
# Quantile list
quantiles = [2.5, 50, 97.5]
# Max OPP in queue, for backpressure if writing is slow
max_opp = 100


def filter_evt_files(files, dbpath, opp_dir, s3=False, worker_count=1,
                     every=10.0):
    """Filter a list of EVT files.

    Positional arguments:
        files - paths to EVT files to filter
        dbpath = SQLite3 db path
        opp_dir = Directory for output binary OPP files

    Keyword arguments:
        s3 - Get EVT data from S3
        worker_count - number of worker processes to use
        every - Percent progress output resolution
    """
    conf = {
        "file": None,  # fill in later
        "s3": s3,
        "cloud_config_items": None,
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "filter_params": None  # fill in later from db
    }

    if not dbpath:
        raise ValueError("Must provide db path to filter_evt_files()")
    if worker_count < 1:
        raise ValueError("worker_count must be > 0")
    if every <= 0 or every > 100:
        raise ValueError("resolution must be > 0 and <= 100")

    worker_count = min(len(files), worker_count)

    filter_df = db.get_latest_filter(dbpath)

    # Turn pandas dataframe into dictionary keyed by quantile for convenience
    conf["filter_params"] = {}
    for q in quantiles:
        conf["filter_params"][q] = dict(filter_df[filter_df["quantile"] == q].iloc[0])
    conf["filter_id"] = filter_df.iloc[0]["id"]

    if s3:
        aws_config = get_aws_config(s3_only=True)
        conf["cloud_config_items"] = aws_config.items("aws")

    # Create input queue with info necessary to filter one file
    files_q = Queue()
    for f in files:
        conf_copy = copy.copy(conf)
        conf_copy["file"] = f
        files_q.put(conf_copy)
    # Put sentinel stop values on the input queue, one for each consumer process
    for i in range(worker_count):
        files_q.put(stop)

    # Create output queues
    stats_q = Queue()  # result stats
    opps_q = Queue(max_opp)   # OPP data

    # Create worker processes
    workers = []
    for i in range(worker_count):
        p = Process(target=do_filter, args=(files_q, opps_q))
        p.daemon = True
        p.start()
        workers.append(p)

    # Create zipping output process
    saver = Process(
        target=do_save,
        args=(opps_q, stats_q, len(files))
    )
    saver.daemon = True
    saver.start()

    # Create reporting process
    reporter = Process(
        target=do_reporting,
        args=(stats_q, len(files), every)
    )
    reporter.daemon = True
    reporter.start()

    # Wait for everything to finish
    for w in workers:
        w.join()
    saver.join()
    reporter.join()


def do_filter(files_q, opps_q):
    """Filter one EVT file, save to sqlite3, return filter stats"""
    conf = files_q.get()
    while conf != stop:
        results = {
            "error": "",
            "conf": conf,
            "quantiles": dict([(q, None) for q in quantiles])
        }

        evt_file = conf["file"]
        fileobj = None
        if conf["s3"]:
            cloud = clouds.AWS(conf["cloud_config_items"])
            fileobj = cloud.download_file_memory(evt_file)

        try:
            evt_ = evt.EVT(path=evt_file, fileobj=fileobj)
        except errors.FileError as e:
            # Make empty evt_, no filtering results
            results["error"] = f"Could not parse file {evt_file}: {e}"
            evt_ = evt.EVT(path=evt_file, fileobj=fileobj, read_data=False)
        except Exception as e:
            # No evt_
            results["error"] = f"Unexpected error for file {evt_file}: {e}"
            evt_ = None
        if evt_:
            for q in quantiles:
                try:
                    opp = evt_.filter(conf["filter_params"][q])
                    results["quantiles"][q] = opp
                except Exception as e:
                    results["error"] = f"Error when filtering file {evt_file}, quantile {q}: {e}"
                    break

        opps_q.put(results)
        conf = files_q.get()
    #print(f"worker {os.getpid()} is exiting", file=sys.stderr)


def do_save(opps_q, stats_q, files_left):
    while files_left > 0:
        res = opps_q.get()
        files_left -= 1
        conf = res["conf"]

        # Save to DB
        if conf["dbpath"]:
            for q, opp in res["quantiles"].items():
                if opp:
                    opp.save_opp_to_db(conf["filter_id"], q, conf["dbpath"])

        # Write to OPP file if all quantiles have focused data
        if conf["opp_dir"]:
            all_quantiles_have_opp = True
            for q, opp in res["quantiles"].items():
                if not opp or opp.particle_count == 0:
                    all_quantiles_have_opp = False
                    break
            if all_quantiles_have_opp:
                for q, opp in res["quantiles"].items():
                    opp.write_binary(conf["opp_dir"], opp=True, quantile=q)
        stats_q.put(res)
    #print(f"saver {os.getpid()} is exiting", file=sys.stderr)


def do_reporting(stats_q, file_count, every):
    evt_count = 0
    evt_signal_count = 0
    opp_count = 0
    files_ok = 0

    print("")
    print(f"Filtering {file_count} EVT files. Progress for 50th quantile every ~ {every}%")

    t0 = time.time()

    last = 0  # Last progress milestone in increments of every
    evt_count_block = 0  # EVT particles in this block (between milestones)
    evt_signal_count_block = 0  # EVT noise filtered particles in this block
    opp_count_block = 0  # OPP particles in this block

    # Filter particles in parallel with process pool
    for i in range(file_count):
        res = stats_q.get()  # get next result

        if res["error"]:
            print(res["error"], file=sys.stderr)
        else:
            files_ok += 1

        opp = res["quantiles"][50]  # only consider 50% quantile for reporting
        if opp:
            evt_count_block += opp.parent.event_count
            evt_signal_count_block += opp.parent.particle_count
            opp_count_block += opp.particle_count

        # Print progress periodically
        perc = float(i + 1) / file_count * 100  # Percent completed
        # Round down to closest every%
        milestone = int(perc / every) * every
        if milestone > last:
            now = time.time()
            evt_count += evt_count_block
            evt_signal_count += evt_signal_count_block
            opp_count += opp_count_block
            ratio_signal_block = util.zerodiv(opp_count_block, evt_signal_count_block)
            msg = f"File: {i + 1}/{file_count} {perc:5.4}%"
            msg += " OPP/EVT particles: %i / %i (%i total events) ratio: %.04f elapsed: %.2fs" % \
                (opp_count_block, evt_signal_count_block, evt_count_block,
                ratio_signal_block, now - t0)
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

    opp_evt_signal_ratio = util.zerodiv(opp_count, evt_signal_count)

    t1 = time.time()
    delta = t1 - t0
    evtrate = util.zerodiv(evt_count, delta)
    evtsignalrate = util.zerodiv(evt_signal_count, delta)
    opprate = util.zerodiv(opp_count, delta)

    print("")
    print(f"Input EVT files = {file_count}")
    print(f"Parsed EVT files = {files_ok}")
    print("EVT particles = %s (%.2f p/s)" % (evt_count, evtrate))
    print("EVT noise filtered particles = %s (%.2f p/s)" % (evt_signal_count, evtsignalrate))
    print("OPP particles = %s (%.2f p/s)" % (opp_count, opprate))
    print("OPP/EVT ratio = %.04f" % opp_evt_signal_ratio)
    print("Filtering completed in %.2f seconds" % (delta,))
    #print(f"reporter {os.getpid()} is exiting", file=sys.stderr)
