import itertools
import multiprocessing as mp
import random

import pandas as pd
from seaflowpy import fileio
from seaflowpy import particleops
from seaflowpy import seaflowfile
from seaflowpy import util


def random_select(things, fraction, seed=None):
    """
    Randomly sample a fraction of items in an iterable.

    Parameters
    ----------
    things: list
        Things to sample from.
    fraction: float
        Fraction of things to sample, between 0 and 1. Things will be randomly
        chosen. At least 1 thing will always be chosen.
    seed: int
        Integer seed for PRNG. If None, a source of random seed will be used.

    Returns
    -------
    list
        Chosen things.
    """
    if (seed is not None) and not isinstance(seed, int):
        raise ValueError("seed must be an None or an int")
    if fraction < 0 or fraction > 1:
        raise ValueError("fraction must be between 0 and 1")

    if len(things) == 0:
        return things

    if seed is not None:
        rand = random.Random(seed)
    else:
        rand = random.Random()
    count = max(int(len(things) * fraction), 1)  # select at least 1 thing
    thing_indexes = list(range(0, len(things)))
    chosen_i = sorted(rand.sample(thing_indexes, count))
    chosen_things = [things[i] for i in chosen_i]
    return chosen_things


def sample(
    evtpaths,
    n,
    outpath,
    dates=None,
    min_chl=0,
    min_fsc=0,
    min_pe=0,
    multi=False,
    noise_filter=False,
    process_count=1,
    seed=None,
):
    """
    Randomly sample rows from EVT files.

    May sample many files separately or as one data set. This function is a
    parallel wrapper for sample_many_to_one.

    Parameters
    ----------
    evtpaths: list of str
        EVT file paths.
    n: int
        Events to keep per file (--multi) or total.
    outpath: str
        Parquet output file.
    dates: dict of {file_id : datetime.datetime}
        If provided, create a column of dates called "date".
    min_chl: int, default 0
        Minimum chl_small value.
    min_fsc: int, default 0
        Minimum fsc_small value.
    min_pe: int, default 0
        Minimum pe value.
    multi: bool, default False
        Output one file per input file. If False, create one aggregated output file.
    noise_filter: bool, default False
        Remove noise particles after sampling.
    process_count: int, default: 1
        Number of worker processes to create.
    seed: int, default None
        Integer seed for PRNG, used in sampling files and events. If None, a
        source of random seed will be used.

    Returns
    -------
    tupe of (list of dicts for each file, unhandled exceptions):
        [
            [{
                "msg": any errors encountered while processing this file,
                "file_id": input EVT file ID,
                "events": event count in original file,
                "events_postfilter": event count after applying min val / noise filters,
                "events_postsampling": event count after subsampling
            }, ...],
            [unhandled exceptions]
    """
    if len(evtpaths) == 0:
        return ([], [])

    # Don't create more processes than input files
    process_count = min(process_count, len(evtpaths))
    # Split EVT files up into buckets, one for each worker process
    file_buckets = util.jobs_parts(evtpaths, process_count)

    # How many events to take per file
    if multi:
        n_per_file = n
    else:
        n_per_file = n // len(evtpaths)

    pool = mp.Pool(processes=process_count)

    # Result handling callbacks for mp.async_apply
    mp_results, mp_errs = [], []

    def cb(result):
        mp_results.append(result)

    def err_cb(err):
        mp_errs.append(err)

    # kwargs for each worker process, same for each
    kwargs = {
        "min_chl": min_chl,
        "min_fsc": min_fsc,
        "min_pe": min_pe,
        "noise_filter": noise_filter,
        "seed": seed,
    }

    for i, bucket_o_files in enumerate(file_buckets):
        args = (i, bucket_o_files, n_per_file)
        pool.apply_async(
            _sample_many_to_one_worker,
            args,
            kwargs,
            callback=cb,
            error_callback=err_cb,
        )
    pool.close()
    pool.join()
    mp_results = sorted(
        mp_results, key=lambda x: x["i"]
    )  # sort async results by orig order
    results = list(
        itertools.chain.from_iterable([r["results"] for r in mp_results])
    )
    df = pd.concat([r["df"] for r in mp_results], ignore_index=True)
    if dates:
        df["date"] = df["file_id"].map(dates)
    df["file_id"] = df["file_id"].astype("category")
    assert len(df.index) == sum([r["events_postsampling"] for r in results])
    df.to_parquet(outpath)

    return (results, mp_errs)


def _sample_many_to_one_worker(i, *args, **kwargs):
    results = sample_many_to_one(*args, **kwargs)
    results["i"] = i  # to sort async result blocks later
    return results


def sample_many_to_one(
    evtpaths, n, min_chl=0, min_fsc=0, min_pe=0, noise_filter=False, seed=None,
):
    """
    Randomly sample rows from EVT files and combined into one dataframe.

    Parameters
    ----------
    evtpaths: list of str
        EVT file paths.
    n: int
        Events to sample from each input file, > 0.
    min_chl: int, default 0
        Minimum chl_small value.
    min_fsc: int, default 0
        Minimum fsc_small value.
    min_pe: int, default 0
        Minimum pe value.
    noise_filter: bool, default False
        Remove noise particles after sampling.
    seed: int, default None
        Integer seed for PRNG, used in sampling files and events. If None, a
        source of random seed will be used.

    Returns
    -------
    dict
        {
            "df": combined dataframe of sampled data from all files,
            "results": [{
                "msg": any errors encountered while processing this file,
                "file_id": input EVT file ID,
                "events": event count in original file,
                "events_postfilter": event count after applying min val / noise filters,
                "events_postsampling": event count after subsampling
            }, ...]
        }
    """
    columns = ["D1", "D2", "fsc_small", "pe", "chl_small"]
    results = []

    for f in evtpaths:
        msg = ""
        try:
            df = fileio.read_evt_labview(f)
        except Exception as e:
            msg = "{}: {}".format(type(e).__name__, str(e))
            df = particleops.empty_df()
        result = sample_one(
            df,
            n,
            min_chl=min_chl,
            min_fsc=min_fsc,
            min_pe=min_pe,
            noise_filter=noise_filter,
            seed=seed,
        )
        result["df"] = result["df"][columns]
        file_id = seaflowfile.SeaFlowFile(f).file_id
        result["df"]["file_id"] = file_id
        result["file_id"] = file_id
        result["msg"] = msg
        results.append(result)

    if len(results):
        df = pd.concat([r["df"] for r in results], ignore_index=True)
        for r in results:
            del r["df"]
    else:
        df = particleops.empty_df()[columns]
        df["file_id"] = None

    return {
        "df": df,
        "results": results,
    }


def sample_one(df, n, noise_filter=True, min_chl=0, min_fsc=0, min_pe=0, seed=None):
    """
    Randomly sample rows from an EVT dataframe.

    Parameters
    ----------
    df: pandas.DataFrame
        EVT dataframe to sample from.
    n: int
        Events to sample from input files, > 0.
    noise_filter: bool, default True
        Remove noise particles after sampling.
    min_chl: int, default 0
        Minimum chl_small value.
    min_fsc: int, default 0
        Minimum fsc_small value.
    min_pe: int, default 0
        Minimum pe value.
    seed: int, default None
        Integer seed for PRNG, used in sampling files and events. If None, a
        source of random seed will be used.

    Raises
    ------
    ValueError

    Returns
    -------
    dict
        {
            "df": subsampled pandas.DataFrame,
            "events": event count in original file,
            "events_postfilter": event count after applying min val / noise filters,
            "events_postsampling": event count after subsampling
        }
    """
    if (seed is not None) and not isinstance(seed, int):
        raise ValueError("seed must be an None or an int")
    if n <= 0:
        raise ValueError("n must be > 0")

    events = len(df.index)
    chl = df["chl_small"].values >= min_chl
    fsc = df["fsc_small"].values >= min_fsc
    pe = df["pe"].values >= min_pe
    if noise_filter:
        noise = particleops.mark_noise(df)
        df = df[(~noise) & chl & fsc & pe]
    else:
        df = df[chl & fsc & pe]
    events_postfilter = len(df.index)
    try:
        frac = min(n / events_postfilter, 1)
    except ZeroDivisionError:
        pass
    else:
        if seed is None:
            df = df.sample(frac=frac)
        else:
            df = df.sample(frac=frac, random_state=seed)

    return {
        "df": df,
        "events": events,
        "events_postfilter": events_postfilter,
        "events_postsampling": len(df.index),
    }
