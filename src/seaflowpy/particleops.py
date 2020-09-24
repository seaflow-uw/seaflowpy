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


def all_quantiles(df):
    """
    Are there particles in all quantiles?

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow particle data with focused particles marked by mark_focused().
        Focused particles should be marked with a boolean column for each
        quantile, where column names are q<quantile>, e.g. q2.5 for 2.5%
        quantile.

    Returns
    ------
    bool
    """
    for q_col in [c for c in df.columns if c.startswith("q")]:
        if not df[q_col].any():
            return False
    return True


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


def mark_focused(df, params, inplace=False):
    """
    Mark focused particle data.

    Adds boolean cols for noise, saturation, and focused particles by quantile.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow raw event DataFrame.
    params: pandas.DataFrame
        Filtering parameters as pandas DataFrame.
    inplace: bool, default False
        Add new booleans columns to and return input DataFrame. If False,
        add new columns to and return a copy of the input DataFrame, leaving the
        original unmodified.

    Returns
    -------
    pandas.DataFrame
        Reference to or copy of input DataFrame with new boolean columns.
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

    if not inplace:
        df = df.copy()

    # Apply noise filter and saturation filter for D1 and D2
    df["noise"] = mark_noise(df)
    df["saturated"] = mark_saturated(df)

    # Filter for aligned/focused particles
    #
    # Filter aligned particles (D1 = D2), with correction for D1 D2
    # sensitivity difference. Assume width is same for all quantiles so just
    # grab first width value and calculate aligned particles once
    assert len(params["width"].unique()) == 1  # may as well check
    width = params.loc[0, "width"]
    alignedD1 = df["D1"].values < (df["D2"].values + width)
    alignedD2 = df["D2"].values < (df["D1"].values + width)
    aligned = ~df["noise"] & ~df["saturated"] & alignedD1 & alignedD2

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

    This function returns a boolean Series marking events where none of
    D1, D2, or fsc_small are > 1.

    Parameters
    ----------
    numpy.ndarray
        Boolean array of noise events.
    """
    if len(set(list(df)).intersection(set(["D1", "D2", "fsc_small"]))) < 3:
        raise ValueError("Can't apply noise filter without D1, D2, and fsc_small")

    # Mark noise events in new column "noise"
    return ~((df["fsc_small"].values > 1) | (df["D1"].values > 1) | (df["D2"].values > 1))


def mark_saturated(df):
    """
    Mark data that saturates D1 or D2.

    This function returns a boolean Series marking events where none of
    D1 == max(D1) or D2 == max(D2).

    Parameters
    ----------
    numpy.ndarray
        Boolean array of saturated events.
    """
    if len(set(list(df)).intersection(set(["D1", "D2"]))) < 2:
        raise ValueError("Can't apply saturation filter without D1 and D2")
    if len(df.index) == 0:
        return np.full(len(df.index), False)
    else:
        return (df["D1"].values == df["D1"].values.max()) | (df["D2"].values == df["D2"].values.max())


def merge_opp_vct(oppdf, vctdf):
    """
    Return a new DataFrame that combines an OPP DataFrame with a VCT DataFrame
    for one quantile.

    Parameters
    ----------
    oppdf: pandas.DataFrame
        SeaFlow OPP data for one quantile.
    vctdf: pandas.DataFrame
        SeaFlow VCT data for one quantile.

    Returns
    -------
    pandas.DataFrame
        New merged DataFrame.
    """
    if len(oppdf) != len(vctdf):
        raise ValueError("oppdf and vctdf must have the same number of rows")
    # Concat will join data by row index, so make sure both dataframes have the
    # same sequential indexes here first.
    oppdf = oppdf.reset_index(drop=True)
    vctdf = vctdf.reset_index(drop=True)
    return pd.concat([oppdf, vctdf], axis=1)


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


def roughfilter(df, width=5000):
    """
    Filter EVT particle data without bead positions or instrument calibration.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    width: int

    Returns
    -------
    pandas.DataFrame
        Copy of subset of df where each row is focused in at least on quantile.
    """
    if width is None:
        raise ValueError("Must supply width to roughfilter")
    # Prevent potential integer division bugs
    width = float(width)

    if len(df) == 0:
        return empty_df()

    # Mark noise and saturated particles
    noise = mark_noise(df)
    sat = mark_saturated(df)

    if np.sum(~noise & ~sat) == 0:
        # All data is noise/saturation filtered
        return empty_df()

    # Correction for the difference in sensitivity between D1 and D2
    origin = (df["D2"] - df["D1"]).median()

    # Filter aligned particles (D1 = D2), with correction for D1 D2
    # sensitivity difference.
    alignedD1 = (df["D1"] + origin) < (df["D2"] + width)
    alignedD2 = df["D2"] < (df["D1"] + origin + width)
    aligned = df[~noise & ~sat & alignedD1 & alignedD2]

    # Find fsc/d ratio (slope) for best large fsc particle
    fsc_small_max = aligned["fsc_small"].max()
    # Smallest D1 with maximum fsc_small
    min_d1 = aligned[aligned["fsc_small"] == fsc_small_max]["D1"].min()
    slope_d1 = fsc_small_max / min_d1
    # Smallest D2 with maximum fsc_small
    min_d2 = aligned[aligned["fsc_small"] == fsc_small_max]["D2"].min()
    slope_d2 = fsc_small_max / min_d2

    # Filter focused particles
    # Better fsc/d signal than best large fsc particle
    oppD1 = (aligned["fsc_small"] / aligned["D1"]) >= slope_d1
    oppD2 = (aligned["fsc_small"] / aligned["D2"]) >= slope_d2
    oppdf = aligned[oppD1 & oppD2].copy()

    return oppdf


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


def linearize_particles(df, columns=None):
    """
    Linearize logged SeaFlow data.

    SeaFlow data is stored as log values over 3.5 decades on a 16-bit linear
    scale. This functions exponentiates those values onto a linear scale from 1
    to 10**3.5

    Note: This will convert to float64 if necessary.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    columns: list of str, default seaflowpy.particleops.channel_columns
        Names of columns to linearize.

    Returns
    -------
    pandas.DataFrame
        Copy of df with linearized values.
    """
    if not columns:
        columns = CHANNEL_COLUMNS
    events = df.copy()
    if len(events.index) > 0:
        events[columns] = 10**((events[columns] / 2**16) * 3.5)
    return events


def log_particles(df, columns=None):
    """
    Opposite of linearize_particles().

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    columns: list of str, default seaflowpy.particleops.channel_columns
        Names of columns to log.

    Returns
    -------
    pandas.DataFrame
        Copy of df with logged values.
    """
    if not columns:
        columns = CHANNEL_COLUMNS
    events = df.copy()
    if len(events.index) > 0:
        events[columns] = (np.log10(events[columns]) / 3.5) * 2**16
        events[columns] = events[columns].round(0)
    return events
