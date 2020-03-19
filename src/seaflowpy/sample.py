import itertools
import multiprocessing as mp
import random

import pandas as pd
from seaflowpy import errors
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
    if not isinstance(seed, int):
        raise ValueError("seed must be an int")
    if fraction < 0 or fraction > 1:
        raise ValueError("fraction must be between 0 and 1")

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

    May sample many files and output many files, or sample many files and output
    one file. This function is a common parallel wrapper for sample_many_to_many
    and sample_many_to_one.

    Parameters
    ----------
    evtpaths: list of str
        EVT file paths.
    n: int
        Events to keep per file (--multi) or total.
    outpath: str
        Output directory in multi mode, or output file in non-multi mode. In multi mode
        output file paths will match input file paths, i.e. day-of-year-dir/filename.
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
    # First split EVT files up into buckets, one for each worker process
    file_buckets = util.jobs_parts(evtpaths, process_count)

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

    if multi:
        for i, bucket_o_files in enumerate(file_buckets):
            args = (i, bucket_o_files, outpath, n)
            pool.apply_async(
                _sample_many_to_many_worker,
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
    else:
        for i, bucket_o_files in enumerate(file_buckets):
            n_per_file = n // len(evtpaths)
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
        fileio.write_labview(df, outpath)
        assert len(df.index) == sum([r["events_postsampling"] for r in results])

    return (results, mp_errs)


def _sample_many_to_one_worker(i, *args, **kwargs):
    results = sample_many_to_one(*args, **kwargs)
    results["i"] = i  # to sort async result blocks later
    return results


def _sample_many_to_many_worker(i, *args, **kwargs):
    results = sample_many_to_many(*args, **kwargs)
    results = {"i": i, "results": results}  # to sort async result blocks later
    return results


def sample_many_to_many(
    evtpaths, outdir, n, min_chl=0, min_fsc=0, min_pe=0, noise_filter=False, seed=None,
):
    """
    Randomly sample rows from EVT files and write to many files.

    Output files be written to outdir with a subtree that matches input files.

    Parameters
    ----------
    evtpaths: list of str
        EVT file paths.
    outdir: str
        Output directory.
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
    list of dicts for each file:
        [{
            "msg": any errors encountered while processing this file,
            "file_id": input EVT file ID,
            "events": event count in original file,
            "events_postfilter": event count after applying min val / noise filters,
            "events_postsampling": event count after subsampling
        }, ...]
    """
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
        fileio.write_evt_labview(result["df"], f, outdir, gz=True)
        del result["df"]
        result["msg"] = msg
        result["file_id"] = seaflowfile.SeaFlowFile(f).file_id
        results.append(result)
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
    results = []
    for f in evtpaths:
        msg = ""
        try:
            df = fileio.read_evt_labview(f)
        except errors.FileError as e:
            msg = str(e)
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
        result["msg"] = msg
        result["file_id"] = seaflowfile.SeaFlowFile(f).file_id
        results.append(result)
    df = pd.concat([r["df"] for r in results], ignore_index=True)
    for r in results:
        del r["df"]
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
    if not isinstance(seed, int):
        raise ValueError("seed must be an int")
    if n <= 0:
        raise ValueError("n must be > 0")

    events = len(df.index)
    chl = df["chl_small"].values >= min_chl
    fsc = df["fsc_small"].values >= min_fsc
    pe = df["pe"].values >= min_pe
    if noise_filter:
        particleops.mark_noise(df)
        df = df[(~df["noise"]) & chl & fsc & pe].drop(columns=["noise"])
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
