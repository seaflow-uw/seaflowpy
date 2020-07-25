import copy
#import datetime
#import os
import sys
import time
import multiprocessing as mp
import queue

from . import clouds
from .conf import get_aws_config
from . import db
from . import errors
from . import fileio
from . import particleops
from . import util


# Stop sentinel value for queues
stop = 'STOP'
# Quantile list
quantiles = [2.5, 50, 97.5]


@util.quiet_keyboardinterrupt
def filter_evt_files(files_df, dbpath, opp_dir, s3=False, worker_count=1,
                     every=10.0, window_size="1H"):
    """Filter a list of EVT files.

    Positional arguments:
        files_df - DataFrame of "file_id", "path", "date" as file ID string,
            file path, and pandas.Timestamp for the file.
        dbpath = SQLite3 db path
        opp_dir = Directory for output binary OPP files

    Keyword arguments:
        s3 - Get EVT data from S3
        worker_count - number of worker processes to use
        every - Percent progress output resolution
        window_size - Time window for grouping filtering EVT file sets,
            expressed as pandas time offsets.
    """
    work = {
        "files_df": None,  # fill in later
        "s3": s3,
        "cloud_config_items": None,
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "filter_params": None,  # fill in later from db,
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
    grouped = files_df.set_index("date").resample(window_size)

    worker_count = min(len(grouped), worker_count)

    work["filter_params"] = db.get_latest_filter(dbpath)

    if s3:
        aws_config = get_aws_config(s3_only=True)
        work["cloud_config_items"] = aws_config.items("aws")

    # Create input queue with info necessary to filter one file
    work_q = mp.Queue()
    # Create output queues
    stats_q = mp.Queue()  # result stats
    opps_q = mp.Queue()   # OPP data
    done_q = mp.Queue()   # signal we're done to main thread

    # Create worker processes
    workers = []
    for _ in range(worker_count):
        p = mp.Process(target=do_filter, args=(work_q, opps_q))
        p.start()
        workers.append(p)

    # Create db output process
    saver = mp.Process(
        target=do_save,
        args=(opps_q, stats_q, len(files_df))
    )
    saver.start()

    # Create reporting process
    reporter = mp.Process(
        target=do_reporting,
        args=(stats_q, done_q, len(files_df), every)
    )
    reporter.start()

    # Add work to the work queue, binned by hour
    for name, group in grouped:
        if len(group) > 0:
            work_copy = copy.deepcopy(work)
            work_copy["files_df"] = group.copy()
            work_copy["window_start_date"] = name
            work_q.put(work_copy)
    # Put sentinel stop values on the input queue, one for each consumer process
    for _ in range(worker_count):
        work_q.put(stop)

    try:
        # Wait for reporter to tell us we're done
        done = done_q.get()
        if done is not None:
            # Something went wrong, shut child processes down
            print(done, file=sys.stderr)
    finally:
        for w in workers:
            w.terminate()
            w.join()
        saver.terminate()
        saver.join()
        reporter.join()


@util.quiet_keyboardinterrupt
def do_filter(work_q, opps_q):
    """Filter one EVT file, save to sqlite3, return filter stats"""
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

            try:
                fileobj = None
                if work["s3"]:
                    cloud = clouds.AWS(work["cloud_config_items"])
                    fileobj = cloud.download_file_memory(row["path"])
                evt_df = fileio.read_evt_labview(path=row["path"], fileobj=fileobj)
            except errors.FileError as e:
                result["error"] = f"Could not parse file {row['path']}: {e}"
                evt_df = particleops.empty_df()
            except Exception as e:
                result["error"] = f"Unexpected error when parsing file {row['path']}: {e}"
                evt_df = particleops.empty_df()

            try:
                evt_df = particleops.mark_focused(evt_df, work["filter_params"], inplace=True)
                opp_df = particleops.select_focused(evt_df)
                opp_df["date"] = date
                opp_df["file_id"] = row["file_id"]
                opp_df["filter_id"] = work["filter_params"]["id"][0]
                result["opp"] = opp_df
                result["all_count"] = len(evt_df.index)
                result["noise_count"] = len(evt_df[evt_df["noise"]].index)
                result["saturated_count"] = len(evt_df[evt_df["saturated"]].index)
                result["opp_count"] = len(opp_df[opp_df["q50"]])
            except Exception as e:
                result["error"] = f"Unexpected error when selecting focused partiles in file {row['path']}: {e}"

            work["results"].append(result)

        # Prep db data
        filter_id = work["filter_params"]["id"].unique().tolist()[0]
        work["opp_vals"], work["outlier_vals"] = [], []
        for r in work["results"]:
            work["opp_vals"].extend(
                db.prep_opp(
                    r["file_id"],
                    r["opp"],
                    r["all_count"],
                    r["all_count"] - r["noise_count"],
                    filter_id
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
        opps_q.put(work)
        #print("{} {} sent {}/{} results at {}".format(work["window_start_date"], os.getpid(), len(work["results"]), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
        work = work_q.get()
        # if work != stop:
        #     print("{} {} got more work {} at {}".format(work["window_start_date"], os.getpid(), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
        # else:
        #     print("{} got stop {}".format(os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)


@util.quiet_keyboardinterrupt
def do_save(opps_q, stats_q, files_left):
    while files_left > 0:
        try:
            work = opps_q.get(True, 600)  # We should get one hour of data every ten minutes at least
            #print("{} {} received {}/{} results at {}".format(work["window_start_date"], os.getpid(), len(work["results"]), len(work["files_df"]), datetime.datetime.now().isoformat()), file=sys.stderr)
        except queue.Empty as e:
            stats_q.put("EMPTY QUEUE")
            break
        except Exception:
            stats_q.put("QUEUE ERROR")
            break

        files_left -= len(work["files_df"])

        # Save to DB
        try:
            if work["dbpath"]:
                if work["opp_vals"]:
                    db.save_opp_to_db(work["opp_vals"], work["dbpath"])
                if work["outlier_vals"]:
                    db.save_outlier(work["outlier_vals"], work["dbpath"])
                #print("{} {} db saved at {}".format(work["window_start_date"], os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)
        except Exception as e:
            work["errors"].append("Unexpected error when saving file {} to db: {}".format(work["file"], e))

        #print("{} {} sent stats at {}".format(work["window_start_date"], os.getpid(), datetime.datetime.now().isoformat()), file=sys.stderr)
        stats_q.put(work)


@util.quiet_keyboardinterrupt
def do_reporting(stats_q, done_q, file_count, every):
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

        if work in ("EMPTY QUEUE", "QUEUE ERROR"):
            # Something went wrong upstream, exit with an error message
            done_q.put(f"A fatal error occurred after filtering {files_seen}/{file_count} files: {work}")
            sys.exit(1)

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
    done_q.put(None)
