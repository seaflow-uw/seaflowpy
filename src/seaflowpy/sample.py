import random
import sys
import pandas as pd
from . import errors
from . import fileio
from . import particleops


def sample(files, n, file_fraction, filter_noise=True, min_chl=0, min_fsc=0,
           min_pe=0, seed=None, verbose=0):
    """
    Randomly sample rows from EVT files.

    Parameters
    ----------
    files: list of str
        EVT file paths.
    n: int
        Rows to sample from input files, > 0.
    file_fraction: float
        Fraction of files to read, between 0 and 1. Files will be randomly
        chosen. At least 1 file will always be chosen.
    filter_noise: bool, default True
        Remove noise particles after sampling.
    min_chl: int
        Minimum chl_small value.
    min_fsc: int
        Minimum fsc_small value.
    min_pe: int
        Minimum pe value.
    seed: int
        Integer seed for PRNG, used in sampling files and rows. If None, a
        source of random seed will be used.
    verbose: int, default 0
        A value of 1 prints basic file and row count information. A value > 1
        prints the paths of the files chosen for sampling.

    Returns
    -------
    pandas.DataFrame
        EVT dataframe of sampled rows.
    """
    if not isinstance(seed, int):
        seed = None
    if not files:
        raise ValueError("No files provided")
    if n <= 0:
        raise ValueError("n must be > 0")
    if file_fraction < 0 or file_fraction > 1:
        raise ValueError("file fraction must be between 0 and 1")

    # Subsample from file list
    if verbose:
        print("{} total files".format(len(files)))
    file_n = max(int(len(files) * file_fraction), 1)  # read at least one file
    random.seed(seed)
    file_indexes = list(range(0, len(files)))
    chosen_i = sorted(random.sample(file_indexes, file_n))
    files = [files[i] for i in chosen_i]
    if verbose:
        print("{} files chosen for sampling".format(file_n))
        if verbose > 1:
            for f in files:
                print(f)

    rows_per_file = n / len(files)
    total_rows = 0
    total_rows_postfilter = 0
    subdf = None
    for f in files:
        try:
            df = fileio.read_evt_labview(f)
        except errors.FileError as e:
            print("Error reading {}: {}".format(f, str(e)), file=sys.stderr)
        else:
            total_rows += len(df.index)
            chl = df["chl_small"].values >= min_chl
            fsc = df["fsc_small"].values >= min_fsc
            pe = df["pe"].values >= min_pe
            if filter_noise:
                df = particleops.mark_noise(df)
                df = df[(~df["noise"]) & chl & fsc & pe].drop(columns=["noise"])
            else:
                df = df[chl & fsc & pe]
            rows = len(df.index)
            if not rows:
                print("No data after noise/min filtering {}".format(f), file=sys.stderr)
                continue
            total_rows_postfilter += rows
            frac = min(rows_per_file / rows, 1)
            if seed is None:
                df = df.sample(frac=frac)
            else:
                df = df.sample(frac=frac, random_state=seed)
            if subdf is None:
                subdf = df
            else:
                subdf = pd.concat([subdf, df])
    if subdf is None:
        raise IOError("No data sampled from chosen files")

    subdf.reset_index(drop=True, inplace=True)  # in case downstream depends on unique row labels

    if verbose:
        print("{} total events".format(total_rows))
        print("{} events after noise/min filtering".format(total_rows_postfilter))
        print("{} events sampled".format(len(subdf.index)))

    return subdf
