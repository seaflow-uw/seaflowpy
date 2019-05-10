import math
import numpy as np
import pandas as pd
from . import util


# Data columns in raw SeaFlow particle DataFrame
COLUMNS = [
    "time", "pulse_width", "D1", "D2", "fsc_small", "fsc_perp", "fsc_big",
    "pe", "chl_small", "chl_big"
]
CHANNEL_COLUMNS = COLUMNS[2:]  # flow cytometer channel data columns

# Focused particle masks by quantile column. These get combined into bit flags
# when storing OPP data in a binary file. e.g. 0b110 (6) means a particle is
# focused in quantiles 50.0 and 97.5 but not 2.5.
flags = {
    "q2.5": 1,
    "q50": 2,
    "q97.5": 4
}


def decode_bit_flags(df):
    """
    Convert "bitflags" column to per-quantile focused particles booleans.

    This removes the "bitflags" column and adds a new boolean column for each
    quantile encoded by the bit flags.

    df: pandas.DataFrame
        SeaFlow focused particle DataFrame with "bitflags" column.

    Returns
    -------
    pandas.DataFrame
        Reference to original modified DataFrame.
    """
    # This sort is ASCII-alphabetical! But that works for the set of quantile
    # column names we have so far. If this changes then do a numeric sort by
    # quantile value.
    bitflags = df["bitflags"]
    df = df.drop(["bitflags"], axis="columns")
    for col, f in sorted(flags.items()):
        df[col] = (bitflags & f) > 0
    return df


def empty_df():
    """
    Create an empty SeaFlow particle DataFrame.

    Returns
    -------
    pandas.DataFrame
    """
    return pd.DataFrame(dtype=float, columns=COLUMNS)


def encode_bit_flags(df):
    """
    Encode a "bitflags" column for per-quantile focused particles booleans.

    This removes the the noise and the per-quantile focused particles boolean
    columns and adds a new column "bitflags" with bit flags values for each
    quantile. See particleops.flags for the flag definitions.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow focused particle DataFrame with boolean columns for each
        quantile.

    Returns
    -------
    pandas.DataFrame
        Reference to original modified DataFrame.
    """
    # Construct bit flags to efficiently capture all quantile flag columns
    bitflags = None
    for col, f in flags.items():
        if bitflags is None:
            bitflags = np.left_shift(df[col], int(math.log(f, 2)))
        else:
            bitflags = bitflags | np.left_shift(df[col], int(math.log(f, 2)))
    df["bitflags"] = bitflags  # new column
    return df


def mark_focused(df, params):
    """
    Mark focused particle data.

    Adds two boolean columns to the original DataFrame: "noise" identifies
    events below the instrument noise floor, and "focused" identifies focused
    particles.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow raw event DataFrame.
    params: pandas.DataFrame
        Filtering parameters as pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        Reference to input DataFrame with boolean columns for noise and
        focused particles by quantile.
    """
    # Check parameters
    param_keys = [
        "width", "notch_small_D1", "notch_small_D2", "notch_large_D1",
        "notch_large_D2", "offset_small_D1", "offset_small_D2",
        "offset_large_D1", "offset_large_D2", "quantile"
    ]
    if params is None:
        raise ValueError("Must provide filtering parameters")
    for k in param_keys:
        if not k in params.columns:
            raise ValueError(f"Missing filter parameter {k} in mark_focused")

    # Apply noise filter
    df = mark_noise(df)

    # Filter for aligned/focused particles
    #
    # Filter aligned particles (D1 = D2), with correction for D1 D2
    # sensitivity difference. Assume width is same for all quantiles so just
    # grab first width value and calculate aligned particles once
    assert len(params["width"].unique()) == 1  # may as well check
    width = params.loc[0, "width"]
    alignedD1 = ~df["noise"].values & (df["D1"].values < (df["D2"].values + width))
    alignedD2 = ~df["noise"].values & (df["D2"].values < (df["D1"].values + width))
    aligned = alignedD1 & alignedD2

    for q in params["quantile"].sort_values():
        p = params[params["quantile"] == q].iloc[0]  # get first row of dataframe as series
        # Filter focused particles
        # Using underlying numpy arrays (values) to construct boolean
        # selector is about 10% faster than using pandas Series
        small_D1 = df["D1"].values <= ((df["fsc_small"].values * p["notch_small_D1"]) + p["offset_small_D1"])
        small_D2 = df["D2"].values <= ((df["fsc_small"].values * p["notch_small_D2"]) + p["offset_small_D2"])
        large_D1 = df["D1"].values <= ((df["fsc_small"].values * p["notch_large_D1"]) + p["offset_large_D1"])
        large_D2 = df["D2"].values <= ((df["fsc_small"].values * p["notch_large_D2"]) + p["offset_large_D2"])
        opp_selector = aligned & ((small_D1 & small_D2) | (large_D1 & large_D2))
        # Mark focused particles
        colname = f"q{util.quantile_str(q)}"
        df[colname] = opp_selector
    return df


def mark_noise(df):
    """
    Mark data below noise threshold.

    This function adds a new boolean column "noise" to the particle DataFrame,
    marking events where none of D1, D2, or fsc_small are > 1.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow raw event data.
    """
    if len(set(list(df)).intersection(set(["D1", "D2", "fsc_small"]))) < 3:
        raise ValueError("Can't apply noise filter without D1, D2, and fsc_small")

    # Mark noise events in new column "noise"
    signal_selector = (df["fsc_small"].values > 1) | (df["D1"].values > 1) | (df["D2"].values > 1)
    df["noise"] = ~signal_selector  # new column
    return df


def select_focused(df):
    """
    Return a DataFrame with particles that are focused at least one quantile.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data that has been marked with mark_focused().

    Returns
    -------
    pandas.DataFrame
        Copy of subset of df where each row is focused in at least on quantile.
    """
    selector = False
    for qcolumn in [c for c in df.columns if c.startswith("q")]:
        selector = selector | df[qcolumn].values
    return df[selector].copy()


def transform_particles(df, columns=None):
    """
    Exponentiate logged SeaFlow data.

    SeaFlow data is stored as log values over 3.5 decades on a 16-bit linear
    scale. This functions exponentiates those values onto a linear scale from 1
    to 10**3.5

    Note: This will convert to float64 if necessary.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    columns: list of str, default seaflowpy.particleops.channel_columns
        Names of columns to transform.

    Returns
    -------
    pandas.DataFrame
        Copy of df with transformed values.
    """
    if not columns:
        columns = CHANNEL_COLUMNS
    events = df.copy()
    if len(events.index) > 0:
        events[columns] = 10**((events[columns] / 2**16) * 3.5)
    return events


def quantiles_in_df(df):
    """
    Generator to iterate through focused particles by quantile.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow particle data with focused particles marked by mark_focused().
        Focused particles should be marked with a boolean column for each
        quantile, where column names are q<quantile>, e.g. q2.5 for 2.5%
        quantile.

    Yields
    ------
    q_col: str
        Name of a single quantile focused boolean column.
    q: float
        Quantile number.
    q_str: str
        String representation of quantile suitable for constructing a filesystem
        path.
    q_df: pandas.DataFrame
        Subset of input DataFrame with only particles marked for the quantile
        defined by q_str.
    """
    for q_col in [c for c in df.columns if c.startswith("q")]:
        q_str = util.quantile_str(float(q_col[1:]))  # after "q"
        q = float(q_str)
        q_df = df[df[q_col]]  # select only focused particles for one quantile
        yield q_col, q, q_str, q_df
