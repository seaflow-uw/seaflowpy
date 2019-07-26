import random
import sys
import pandas as pd
from . import errors
from . import fileio
from . import particleops


def sample(files, n, file_fraction, filter_noise=True, seed=None, verbose=0):
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

    # Count all events in selected files
    total_rows = 0
    for f in files:
        try:
            total_rows += fileio.read_labview_row_count(f)
        except errors.FileError:
            pass
    if total_rows == 0:
        raise IOError("No data could be read from chosen files")

    if total_rows < n:
        frac = 1.0
    else:
        frac = n / total_rows

    if verbose:
        print("{} total events".format(total_rows))

    subdf = None
    for f in files:
        try:
            if seed is None:
                df = fileio.read_evt_labview(f).sample(frac=frac)
            else:
                df = fileio.read_evt_labview(f).sample(frac=frac, random_state=seed)
        except errors.FileError as e:
            print("Error reading {}: {}".format(f, str(e)), file=sys.stderr)
        else:
            if subdf is None:
                subdf = df
            else:
                subdf = pd.concat([subdf, df])
    if subdf is None:
        raise IOError("No data could be read from chosen files")

    subdf.reset_index(drop=True, inplace=True)  # in case downstream depends on unique row labels

    if verbose:
        print("{} events sampled".format(len(subdf.index)))

    if filter_noise:
        subdf = particleops.mark_noise(subdf)
        subdf = subdf[~subdf["noise"]].drop(columns=["noise"])
        if verbose:
            print("{} events after noise filtering".format(len(subdf.index)))

    return subdf
