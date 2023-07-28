import copy
#import datetime
import logging
import signal
import sys
import time
import multiprocessing as mp
from queue import Empty

from . import db
from . import errors
from . import fileio
from . import particleops
from . import util

logger = logging.getLogger(__name__)

proc_polling_int_sec = 0.5
# Track any signals received while filtering
signal_received = { "signum": None }
# Stop sentinel value for filtering procs
# Done sentinel value to indicate pipeline finished without an error
stop = "STOP"
done = "DONE"
# Quantile list
quantiles = [2.5, 50, 97.5]
max_particles_per_file_default = 50000 * 180  # max event rate (per sec) 50k


def signal_handler(signum, frame):
    logger.debug("got %s in signal handler", signum)
    signal_received["signum"] = signum


def raise_signal_exception():
    if signal_received["signum"] == signal.SIGINT:
        logger.debug("raising %s", KeyboardInterrupt)
        raise KeyboardInterrupt
    elif signal_received["signum"] == signal.SIGTERM:
        exc = SystemExit(128 + signal_received["signum"])
        logger.debug("raising %s", exc)
        raise exc


def filter_evt_files(files_df, dbpath, opp_dir, worker_count=1, every=10.0,
                     max_particles_per_file=max_particles_per_file_default, window_size="1H"):
    """Filter a list of EVT files.

    Positional arguments:
        files_df - DataFrame of "file_id", "path", "date" as file ID string,
            file path, and pandas.Timestamp for the file.
        dbpath = SQLite3 db path
        opp_dir = Directory for output binary OPP files

    Keyword arguments:
        worker_count - number of worker processes to use
        every - Percent progress output resolution
        window_size - Time window for grouping filtering EVT file sets,
            expressed as pandas time offsets.
    """
    logger.debug("main process started")

    work = {
        "files_df": None,  # fill in later
        "cloud_config_items": None,
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "filter_params": None,  # fill in later from db,
        "max_particles_per_file": max_particles_per_file,
        "window_size": window_size,
        "window_start_date": None,
        "errors": [],  # global errors outside of processing single files
        "results": []
    }

    if not dbpath:
        raise ValueError("Must provide db path to filter_evt_files()")
    if worker_count < 1:
        raise ValueError("worker_count must be > 0")
    if every <= 0 or every > 100:
        raise ValueError("resolution must be > 0 and <= 100")

    # Group by window_size
    by_hour = files_df.set_index("date").resample(window_size)

    worker_count = min(len(by_hour), worker_count)

    # Get a dictionary of file_id: filter_params
    work["filter_params"] = db.get_filter_params_lookup(dbpath, files_df)

    with mp.Manager() as manager:
        # Create input queue with info necessary to filter one file
        work_q = manager.Queue()
        # Create output queues
        # Filtered data (without OPP), filter procs write, db saver proc reads
        filtered_q = manager.Queue()
        # reporting stats, db saver proc writes, reporter proc reads
        stats_q = manager.Queue()
        # Queue to receive done sentinel or child exception
        done_q = manager.Queue()

        # Create worker processes
        workers = []
        for _ in range(worker_count):
            p = mp.Process(
                target=do_filter,
                args=(work_q, filtered_q, done_q),
                daemon=True
            )
            p.start()
            workers.append(p)

        # Create db output process
        saver = mp.Process(
            target=do_save,
            args=(filtered_q, stats_q, len(files_df), done_q),
            daemon=True
        )
        saver.start()

        # Create reporting process
        reporter = mp.Process(
            target=do_reporting,
            args=(stats_q, every, len(files_df), done_q),
            daemon=True
        )
        reporter.start()

        # Now set signal handlers without affecting child procs
        orig_handlers = {
            signal.SIGTERM: signal.signal(signal.SIGTERM, signal_handler),
            signal.SIGINT: signal.signal(signal.SIGINT, signal_handler)
        }

        # Add work to the work queue, binned by hour
        for name, group in by_hour:
            if len(group) > 0:
                work_copy = copy.deepcopy(work)
                work_copy["files_df"] = group.copy()
                work_copy["window_start_date"] = name
                work_q.put(work_copy)
        # Put sentinel stop values on the input queue, one for each consumer process
        for _ in range(worker_count):
            work_q.put(stop)

        try:
            while True:
                # Check for child proc error
                # Polling interval implented here
                try:

                    child_resp = done_q.get(block=True, timeout=proc_polling_int_sec)
                except Empty:
                    child_resp = None
                if child_resp is not None:
                    if child_resp == done:
                        # Normal exit, don't terminate children, expect them to
                        # wrap up on their own.
                        logger.debug("got %s from child, exiting early", done)
                        break
                    else:
                        # Got error from child
                        logger.debug("got error '%s' from child, exiting early", child_resp)
                        for w in workers:
                            w.terminate()
                        saver.terminate()
                        reporter.terminate()
                        break
                elif signal_received["signum"] is not None:
                    # Got signal to exit
                    logger.debug("got signal %s, exiting early", signal_received["signum"])
                    for w in workers:
                        w.terminate()
                    saver.terminate()
                    reporter.terminate()
                    break

            # Drain queues before joining child procs to avoid possible deadlock
            # https://docs.python.org/3/library/multiprocessing.html#pipes-and-queues
            logger.debug("draining queues in main")
            drain_qs(work_q, filtered_q, stats_q)

            logger.debug("joining child procs")
            for w in workers:
                w.join()
            saver.join()
            reporter.join()
            logger.debug("child procs joined")
        finally:
            # Always restore the original signal handlers
            logger.debug("restoring original signal handlers")
            for sig, handler in orig_handlers.items():
                signal.signal(sig, handler)
            # Raise exception that would have been caused by signal, or not if
            # no recognized signal was raised.
            raise_signal_exception()
            # Raise exception reported by child
            if isinstance(child_resp, Exception):
                raise child_resp



def do_filter(work_q, filtered_q, done_q):
    """Filter one EVT file, save to sqlite3, return filter stats"""
    try:
        logger.debug("filter process started")
        work = work_q.get()
        while work != stop:
            #print("{} {} starting {} at {}".format(work["window_start_date"], os.getpid(), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
            for date, row in work["files_df"].iterrows():
                result = {
                    "error": "",
                    "all_count": 0,
                    "evt_count": 0,
                    "saturated_count": 0,
                    "opp": None,
                    "file_id": row["file_id"],
                    "path": row["path"]
                }

                filter_params = work["filter_params"][row["file_id"]].reset_index(drop=True)

                # First check that particle count is below limit
                try:
                    row_count = fileio.read_evt_metadata(row['path'])["rowcnt"]
                except (errors.FileError, IOError) as e:
                    result["error"] = f"Could not parse file {row['path']}: {e}"
                    evt_df = particleops.empty_df()
                except Exception as e:
                    result["error"] = f"Unexpected error when parsing file {row['path']}: {e}"
                    evt_df = particleops.empty_df()
                if row_count > work["max_particles_per_file"]:
                    result["error"] = f"{row_count} records in {row['path']} is > limit of {work['max_particles_per_file']}, will not filter"
                    evt_df = particleops.empty_df()
                elif not result["error"]:
                    # Particle count below limit and file is probably readable, go ahead and filter
                    try:
                        data = fileio.read_evt(row["path"])
                        evt_df = data["df"]
                        result["all_count"] = len(evt_df)
                    except (errors.FileError, IOError) as e:
                        result["error"] = f"Could not parse file {row['path']}: {e}"
                        evt_df = particleops.empty_df()  # doesn't matter if v1 or v2 column composition
                    except Exception as e:
                        result["error"] = f"Unexpected error when parsing file {row['path']}: {e}"
                        evt_df = particleops.empty_df()  # doesn't matter if v1 or v2 column composition

                try:
                    evt_df = particleops.mark_focused(evt_df, filter_params, inplace=True)
                    opp_df = particleops.select_focused(evt_df)
                except Exception as e:
                    result["error"] = f"Unexpected error when marking and selecting focused particles in file {row['path']}: {e}"
                else:
                    opp_df["date"] = date
                    opp_df["file_id"] = row["file_id"]
                    opp_df["filter_id"] = filter_params["id"][0]
                    result["opp"] = opp_df
                    result["noise_count"] = len(evt_df[evt_df["noise"]].index)
                    result["saturated_count"] = len(evt_df[evt_df["saturated"]].index)
                    result["opp_count"] = len(opp_df[opp_df["q50"]])
                    result["evt_count"] = result["all_count"] - result["noise_count"]
                    result["filter_id"] = filter_params["id"][0]

                work["results"].append(result)

            # Prep db data
            work["opp_vals"], work["outlier_vals"] = [], []
            for r in work["results"]:
                work["opp_vals"].extend(
                    db.prep_opp(
                        r["file_id"],
                        r["opp"],
                        r["all_count"],
                        r["evt_count"],
                        r["filter_id"]
                    )
                )

                work["outlier_vals"].extend(db.prep_outlier(r["file_id"], 0))
            # Save OPP file
            # Only include OPP files with data in all quantiles
            good_opps = []
            for r in work["results"]:
                if (not r["error"]) and particleops.all_quantiles(r["opp"]):
                    good_opps.append(r["opp"])
            if (len(good_opps)):
                #print("{} {} saving parquet at {}".format(work["window_start_date"], os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)
                try:
                    if work["opp_dir"]:
                        fileio.write_opp_parquet(
                            good_opps,
                            work["window_start_date"],
                            work["window_size"],
                            work["opp_dir"]
                        )
                except Exception as e:
                    work["errors"].append(f"Unexpected error when saving OPP for {work['window_start_date']}: {e}")
            else:
                work["errors"].append(f"No OPPs had data in all quantiles for {work['window_start_date']}")

            # Erase OPP from payload
            for r in work["results"]:
                del r["opp"]

            #print("{} {} sending {}/{} results at {}".format(work["window_start_date"], os.getpid(), len(work["results"]), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
            filtered_q.put(work)
            #print("{} {} sent {}/{} results at {}".format(work["window_start_date"], os.getpid(), len(work["results"]), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
            work = work_q.get()
            # if work != stop:
            #     print("{} {} got more work {} at {}".format(work["window_start_date"], os.getpid(), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
            # else:
            #     print("{} got stop {}".format(os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)
        if work == stop:
            logger.debug("filter process saw STOP")
    except Exception as e:
        # Something unexpected happened, tell other processes to exit and exit this process
        logger.debug("filter process encountered error '%s'", e)
        done_q.put(e)
    except KeyboardInterrupt:
        # Return quietly on SIGINT, no stack trace
        pass


def do_save(filtered_q, stats_q, files_left, done_q):
    try:
        logger.debug("saver process started")
        while files_left > 0:
            work = filtered_q.get()
                #print("{} {} received {}/{} results at {}".format(work["window_start_date"], os.getpid(), len(work["results"]), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)

            files_left -= len(work["files_df"])

            # Save to DB
            if work["dbpath"]:
                if work["opp_vals"]:
                    db.save_opp_to_db(work["opp_vals"], work["dbpath"])
                if work["outlier_vals"]:
                    db.save_outlier(work["outlier_vals"], work["dbpath"])
                #print("{} {} db saved at {}".format(work["window_start_date"], os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)

            #print("{} {} sent stats at {}".format(work["window_start_date"], os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)
            stats_q.put(work)
    except Exception as e:
        # Something unexpected happened, tell other processes to exit and exit this process
        logger.debug("db saver process encountered error '%s'", e)
        done_q.put(e)
    except KeyboardInterrupt:
        # Return quietly on SIGINT, no stack trace
        pass


def do_reporting(stats_q, every, file_count, done_q):
    try:
        logger.debug("reporter process started")
        event_count = 0
        noise_count = 0
        signal_count = 0
        saturated_count = 0
        opp_count = 0
        files_ok = 0

        print("")
        print(f"Filtering {file_count} EVT files. Progress for 50th quantile every ~ {every}%")

        t0 = time.time()

        last = 0  # Last progress milestone in increments of every
        event_count_block = 0  # EVT particles in this block (between milestones)
        noise_count_block = 0  # EVT noise particles in this block
        signal_count_block = 0  # EVT signal (not noise) particles in this block
        saturated_count_block = 0  # particles saturating D1 or D2
        opp_count_block = 0  # OPP particles in this block
        files_seen = 0
        files_left = file_count

        # Filter particles in parallel with process pool
        while files_left > 0:
            work = stats_q.get()  # get next result group

            #print("{} {} received stats at {}".format(work["window_start_date"], os.getpid(), time.time()), file=sys.stderr)

            files_left -= len(work["files_df"])

            if work["errors"]:
                for e in work["errors"]:
                    print(e, file=sys.stderr)

            for r in work["results"]:
                files_seen += 1

                if r["error"]:
                    print(r["error"], file=sys.stderr)
                else:
                    files_ok += 1

                event_count_block += r["all_count"]
                noise_count_block += r["noise_count"]
                signal_count_block = event_count_block - noise_count_block
                saturated_count_block += r["saturated_count"]
                opp_count_block += r["opp_count"]

                # Print progress periodically
                perc = float(files_seen) / file_count * 100  # Percent completed
                # Round down to closest every%
                milestone = int(perc / every) * every
                if milestone > last:
                    event_count += event_count_block
                    noise_count += noise_count_block
                    signal_count += signal_count_block
                    saturated_count += saturated_count_block
                    opp_count += opp_count_block
                    ratio_noise_block = util.zerodiv(noise_count_block, event_count_block)
                    ratio_saturated_block = util.zerodiv(saturated_count_block, event_count_block)
                    ratio_evtopp_block = util.zerodiv(opp_count_block, signal_count_block)
                    msg = f"File: {files_seen}/{file_count} {perc:5.4}%"
                    msg += " events: %d noise: %d (%.04f) sat: %d (%.04f) opp: %d (%.04f) t: %.2fs" % \
                        (
                            event_count_block,
                            noise_count_block, ratio_noise_block,
                            saturated_count_block, ratio_saturated_block,
                            opp_count_block, ratio_evtopp_block,
                            time.time() - t0
                        )
                    print(msg)
                    sys.stdout.flush()
                    last = milestone
                    event_count_block = 0
                    noise_count_block = 0
                    signal_count_block = 0
                    saturated_count_block = 0
                    opp_count_block = 0

        # If any particle count data is left, add it to totals
        event_count += event_count_block
        noise_count += noise_count_block
        signal_count += signal_count_block
        saturated_count += saturated_count_block
        opp_count += opp_count_block

        ratio_noise = util.zerodiv(noise_count, event_count)
        ratio_saturated = util.zerodiv(saturated_count, event_count)
        ratio_evtopp = util.zerodiv(opp_count, signal_count)

        summary_text = "Total events: %d noise: %d (%.04f) sat: %d (%.04f) opp: %d (%.04f) t: %.2fs" % \
            (
                event_count,
                noise_count, ratio_noise,
                saturated_count, ratio_saturated,
                opp_count, ratio_evtopp,
                time.time() - t0
            )
        print(summary_text)
        print(f"{files_ok} / {file_count} EVT files parsed successfully")
        done_q.put(done)  # signal pipeline finished normally
    except Exception as e:
        # Something unexpected happened, tell other processes to exit and exit this process
        logger.debug("reporter process encountered error '%s'", e)
        done_q.put(e)
    except KeyboardInterrupt:
        # Return quietly on SIGINT, no stack trace
        pass


def drain_qs(*args):
    """Drain items from queues.

    This assumes no other proc / thread is adds stuff to the queue later.
    """
    for q in args:
        try:
            while True:
                _ = q.get(False)
        except Empty:
            pass
