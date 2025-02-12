from __future__ import annotations
import copy
import logging
import sys
import time
import multiprocessing as mp
from typing import TYPE_CHECKING, TypedDict
if TYPE_CHECKING:
    import datetime

import pandas as pd
# from joblib import Parallel, parallel_config, delayed
from . import db
from . import errors
from . import fileio
from . import particleops
from .seaflowfile import SeaFlowFile
from . import util

logger = logging.getLogger(__name__)

MAX_PARTICLES_PER_FILE_DEFAULT = 50000 * 180  # max event rate (per sec) 50k
WINDOW_SIZE = "1H"
RESAMPLE_WINDOW_SIZE = "1h"  # pandas frequency strings use 'h' for hour

def filter_evt_files(files_df, dbpath, opp_dir, worker_count=1, every=10.0,
                     max_particles_per_file=MAX_PARTICLES_PER_FILE_DEFAULT,
                     use_numba=False):
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
        use_numba - Use numba filtering implementation
    """
    if not dbpath:
        raise ValueError("Must provide db path to filter_evt_files()")
    if worker_count < 1:
        raise ValueError("worker_count must be > 0")
    if every <= 0 or every > 100:
        raise ValueError("resolution must be > 0 and <= 100")

    work_template = {
        "files_df": None,  # fill in later
        "dbpath": dbpath,
        "opp_dir": opp_dir,
        "filter_params": None,  # fill in later from db,
        "max_particles_per_file": max_particles_per_file,
        "window_size": WINDOW_SIZE,
        "window_start_date": None,
        "use_numba": use_numba,
        "errors": [],  # global errors outside of processing single files
        "results": []
    }
    # Get a dictionary of file_id: filter_params
    filter_params = db.get_filter_params_lookup(dbpath, files_df)

    # Group by window_size
    files_by_hour = files_df.set_index("date").resample(RESAMPLE_WINDOW_SIZE)

    # Adjust worker count
    worker_count = min(len(files_by_hour), worker_count)

    print("creating work list", flush=True)
    # Create work generator
    work_list = []
    for name, group in files_by_hour:
        if len(group) > 0:
            work = copy.deepcopy(work_template)
            work["files_df"] = group.copy()
            work["window_start_date"] = name
            work["filter_params"] = {}
            for file_id in group["file_id"].to_list():
                work["filter_params"][file_id] = filter_params[file_id]
            work_list.append(work)

    print("", flush=True)
    print(f"Filtering {len(files_df)} EVT files. Progress for 50th quantile every ~ {every}%", flush=True)
    reporter = WorkReporter(len(files_df), every, n_jobs=worker_count)
    if worker_count == 1:
        for work_result in map(do_filter, work_list):
            reporter.register(work_result)
            save_to_db(work_result)
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=worker_count) as pool:
            for work_result in pool.imap(do_filter, work_list):
                reporter.register(work_result)
                save_to_db(work_result)
    reporter.finalize()

    # Switch to joblib when this issue is resolved
    # https://github.com/joblib/joblib/issues/883
    # Worker processes are getting unnecessarily killed with a warning message
    # parallel = Parallel(n_jobs=worker_count, return_as="generator")
    # result_gen = parallel(delayed(do_filter)(work) for work in work_list)
    # print("")
    # print(f"Filtering {len(files_df)} EVT files. Progress for 50th quantile every ~ {every}%")
    # reporter = WorkReporter(len(files_df), every)
    # for work_result in result_gen:
    #     reporter.register(work_result)
    #     save_to_db(work_result)
    # reporter.finalize()


class FileTiming(TypedDict):
    """Class to track per-file filtering performance timings (sec)"""
    file_id: str
    read: float
    focused: float
    select: float
    total: float


class WindowTiming(TypedDict):
    """Class to track per-time-window filtering performance timings (sec)"""
    window: datetime.datetime
    filter: float
    db_prep: float
    write_opp: float
    total: float


class Timings(TypedDict):
    files: list[FileTiming]
    windows: list[WindowTiming]


def do_filter(work):
    """Filter EVT files, save OPP parquet, return filtering stats"""
    if work["use_numba"]:
        filter_func = particleops.mark_focused_fast
    else:
        filter_func = particleops.mark_focused
    # Track timing off different steps in the filtering process to aid
    # in performance optimizations.
    timings: Timings = { "files": [], "windows": [] }

    t0 = time.perf_counter()
    for date, row in work["files_df"].iterrows():
        t0_loop = time.perf_counter()
        file_timing: FileTiming = {
            "file_id": row["file_id"],
            "read": 0,
            "focused": 0,
            "select": 0,
            "total": 0
        }
        timings["files"].append(file_timing)

        filter_params = work["filter_params"][row["file_id"]].reset_index(drop=True)

        result = {
            "error": "",
            "all_count": 0,
            "evt_count": 0,
            "saturated_count": 0,
            "opp": None,
            "file_id": row["file_id"],
            "path": row["path"],
            "filter_id": filter_params.at[0, "id"]
        }

        evt_df = particleops.empty_df()  # doesn't matter if v1 or v2 column composition
        row_count = 0
        max_particles_per_file_reject = False

        # First check that particle count is below limit
        try:
            row_count = fileio.read_evt_metadata(row['path'])["rowcnt"]
        except (errors.FileError, IOError) as e:
            result["error"] = f"Could not parse file {row['path']}: {e}"
        except Exception as e:
            result["error"] = f"Unexpected error when parsing file {row['path']}: {e}"
        else:
            if row_count > work["max_particles_per_file"]:
                result["error"] = f"{row_count} records in {row['path']} > limit ({work['max_particles_per_file']}), will not filter"
                max_particles_per_file_reject = True
                result["all_count"] = row_count
        if not result["error"]:
            # Particle count below limit and file is probably readable, read it
            try:
                # Set EVT dataframe with real data
                evt_df = fileio.read_evt(row["path"])["df"]
                result["all_count"] = len(evt_df)
            except (errors.FileError, IOError) as e:
                result["error"] = f"Could not parse file {row['path']}: {e}"
            except Exception as e:
                result["error"] = f"Unexpected error when parsing file {row['path']}: {e}"
        
        file_timing["read"] = time.perf_counter() - t0_loop
        t1_loop = time.perf_counter()

        # Filter
        try:
            evt_df = filter_func(evt_df, filter_params, inplace=True)
            file_timing["focused"] = time.perf_counter() - t1_loop
            t2_loop = time.perf_counter()
            opp_df = particleops.select_focused(evt_df)
            file_timing["select"] = time.perf_counter() - t2_loop
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
            if not max_particles_per_file_reject:
                result["evt_count"] = result["all_count"] - result["noise_count"]
        work["results"].append(result)
        file_timing["total"] = time.perf_counter() - t0_loop

    window_timing: WindowTiming = {
        "window": work["window_start_date"],
        "filter": time.perf_counter() - t0,
        "db_prep": 0,
        "write_opp": 0,
        "total": 0
    }
    timings["windows"].append(window_timing)

    # Prep db data
    t1 = time.perf_counter()
    work["opp_stat_dfs"], work["outlier_vals"] = [], []
    for r in work["results"]:
        work["opp_stat_dfs"].append(
            db.prep_opp(
                r["file_id"],
                r["opp"],
                r["all_count"],
                r["evt_count"],
                r["filter_id"]
            )
        )
        work["outlier_vals"].append({
            "file": SeaFlowFile(r["file_id"]).file_id,
            "flag": 0
        })
    window_timing["db_prep"] = time.perf_counter() - t1

    t2 = time.perf_counter()
    # Save OPP file
    # Only include OPP files with data in all quantiles
    good_opps = []
    for r in work["results"]:
        if (not r["error"]) and particleops.all_quantiles(r["opp"]):
            good_opps.append(r["opp"])
    if (len(good_opps)):
        if work["opp_dir"]:
            fileio.write_opp_parquet(
                good_opps,
                work["window_start_date"],
                work["opp_dir"]
            )
    else:
        work["errors"].append(f"No OPPs had data in all quantiles for {work['window_start_date']}")
    window_timing["write_opp"] = time.perf_counter() - t2

    # Erase OPP from payload
    for r in work["results"]:
        del r["opp"]
    
    window_timing["total"] = time.perf_counter() - t0
    work["timings"] = timings

    return work


def save_to_db(work):
    # Save to DB
    if work["dbpath"]:
        if work["opp_stat_dfs"]:
            opp = pd.concat(work["opp_stat_dfs"], ignore_index=True)
            db.save_df(opp, "opp", work["dbpath"], clear=False)
        if work["outlier_vals"]:
            db.save_df(pd.DataFrame(work["outlier_vals"]), "outlier", work["dbpath"], clear=False)


class WorkReporter:
    """Class to report on filtering work as it completes"""

    def __init__(self, file_count, every, n_jobs=1):
        self.event_count = 0
        self.noise_count = 0
        self.signal_count = 0
        self.saturated_count = 0
        self.opp_count = 0
        self.file_count = file_count
        self.files_left = file_count
        self.files_ok = 0
        self.files_seen = 0
        self.every = every
        self.n_jobs = n_jobs

        self.last = 0  # Last progress milestone in increments of every
        self.event_count_block = 0  # EVT particles in this block (between milestones)
        self.noise_count_block = 0  # EVT noise particles in this block
        self.signal_count_block = 0  # EVT signal (not noise) particles in this block
        self.saturated_count_block = 0  # particles saturating D1 or D2
        self.opp_count_block = 0  # OPP particles in this block
        self.ratio_noise_block = 0.0  # noise ratio in this block
        self.ratio_saturated_block = 0.0  # saturated ratio in this block
        self.ratio_evtopp_block = 0.0  # OPP/EVT ratio in this block

        self.ratio_noise = 0.0  # total noise ratio
        self.ratio_saturated = 0.0  # total saturated ratio
        self.ratio_evtopp = 0.0  # total EVT/OPP ratio

        self.timings = []

        self.t0 = time.time()

    def register(self, work):
        if work["timings"]:
            self.timings.append(work["timings"])

        self.files_left -= len(work["files_df"])

        if work["errors"]:
            for e in work["errors"]:
                print(e, file=sys.stderr)

        for r in work["results"]:
            self.files_seen += 1

            if r["error"]:
                print(r["error"], file=sys.stderr)
            else:
                self.files_ok += 1

            self.event_count_block += r["all_count"]
            self.noise_count_block += r["noise_count"]
            self.signal_count_block = self.event_count_block - self.noise_count_block
            self.saturated_count_block += r["saturated_count"]
            self.opp_count_block += r["opp_count"]

            # Print progress periodically
            perc = float(self.files_seen) / self.file_count * 100  # Percent completed
            # Round down to closest every%
            milestone = int(perc / self.every) * self.every
            if milestone > self.last:
                self.event_count += self.event_count_block
                self.noise_count += self.noise_count_block
                self.signal_count += self.signal_count_block
                self.saturated_count += self.saturated_count_block
                self.opp_count += self.opp_count_block
                self.ratio_noise_block = util.zerodiv(self.noise_count_block, self.event_count_block)
                self.ratio_saturated_block = util.zerodiv(self.saturated_count_block, self.event_count_block)
                self.ratio_evtopp_block = util.zerodiv(self.opp_count_block, self.signal_count_block)
                msg = f"File: {self.files_seen}/{self.file_count} {perc:5.4}%"
                msg += " events: %d noise: %d (%.04f) sat: %d (%.04f) opp: %d (%.04f) t: %.2fs" % \
                    (
                        self.event_count_block,
                        self.noise_count_block, self.ratio_noise_block,
                        self.saturated_count_block, self.ratio_saturated_block,
                        self.opp_count_block, self.ratio_evtopp_block,
                        time.time() - self.t0
                    )
                print(msg)
                sys.stdout.flush()
                self.last = milestone
                self.event_count_block = 0
                self.noise_count_block = 0
                self.signal_count_block = 0
                self.saturated_count_block = 0
                self.opp_count_block = 0

    def finalize(self):
        # If any particle count data is left, add it to totals
        self.event_count += self.event_count_block
        self.noise_count += self.noise_count_block
        self.signal_count += self.signal_count_block
        self.saturated_count += self.saturated_count_block
        self.opp_count += self.opp_count_block

        self.ratio_noise = util.zerodiv(self.noise_count, self.event_count)
        self.ratio_saturated = util.zerodiv(self.saturated_count, self.event_count)
        self.ratio_evtopp = util.zerodiv(self.opp_count, self.signal_count)

        summary_text = "Total events: %d noise: %d (%.04f) sat: %d (%.04f) opp: %d (%.04f) t: %.2fs" % \
            (
                self.event_count,
                self.noise_count, self.ratio_noise,
                self.saturated_count, self.ratio_saturated,
                self.opp_count, self.ratio_evtopp,
                time.time() - self.t0
            )
        print(summary_text)
        print(f"{self.files_ok} / {self.file_count} EVT files parsed successfully")

        # Timing
        file_timings, window_timings = [], []
        for tim in self.timings:
            file_timings.extend(tim["files"])
            window_timings.extend(tim["windows"])
        file_timings_df = pd.DataFrame(file_timings).select_dtypes("number")
        file_timings_norm_df = file_timings_df / self.n_jobs
        window_timings_df = pd.DataFrame(window_timings).select_dtypes("number")
        window_timings_norm_df = window_timings_df / self.n_jobs
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print("")
            print("Per-file performance timings totals (sec)")
            print(file_timings_df.select_dtypes("float").agg(["median", "sum"]).to_string())
            print("")
            print(f"Divided by job count (/{self.n_jobs})")
            print(file_timings_norm_df.select_dtypes("float").agg(["median", "sum"]).to_string())
            print("#################################################")
            print("Per-time-window performance timings totals (sec)")
            print(window_timings_df.select_dtypes("float").agg(["median", "sum"]).to_string())
            print("")
            print(f"Divided by job count (/{self.n_jobs})")
            print(window_timings_norm_df.select_dtypes("float").agg(["median", "sum"]).to_string())
