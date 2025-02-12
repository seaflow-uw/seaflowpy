import numba
import numpy as np
import numpy.typing as npt
import pandas as pd
from . import util


# Data columns in raw SeaFlow particle DataFrame
COLUMNS = [
    "time", "pulse_width", "D1", "D2", "fsc_small", "fsc_perp", "fsc_big",
    "pe", "chl_small", "chl_big"
]
CHANNEL_COLUMNS = COLUMNS[2:]  # flow cytometer channel data columns
COLUMNS2 = [
    "pulse_width", "chl_small", "D1", "D2", "fsc_small", "pe", "evt_rate"
]
CHANNEL_COLUMNS2 = COLUMNS2[1:6]  # flow cytometer channel data columns

# Reduced column set for Parquet EVT files
REDUCED_COLUMNS = ["D1", "D2", "fsc_small", "pe", "chl_small"]


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


def empty_df(v2=False):
    """
    Create an empty SeaFlow particle DataFrame.

    Returns
    -------
    pandas.DataFrame
    """
    cols = COLUMNS2 if v2 else COLUMNS
    return pd.DataFrame(dtype=float, columns=cols)


def evt_as_np(df: pd.DataFrame) -> dict[str, npt.NDArray[np.float32]]:
    dtype = "float32"
    data = dict()
    data["d1"] = df["D1"].astype(dtype).to_numpy()
    data["d2"] = df["D2"].astype(dtype).to_numpy()
    data["fsc"] = df["fsc_small"].astype(dtype).to_numpy()
    return data


def params_as_np(df: pd.DataFrame) -> dict[str, npt.NDArray[np.float32]]:
    dtype = "float32"
    data = dict()
    df = df.reset_index(drop=True)
    if len(df["width"].unique()) != 1:
        # May as well check
        raise ValueError("only one width allowed in params df")
    data["width"] = df["width"].astype(dtype).to_numpy()
    data["snotch"] = np.array([df["notch_small_D1"].astype(dtype).to_numpy(), df["notch_small_D2"].astype(dtype).to_numpy()]).T
    data["lnotch"] = np.array([df["notch_large_D1"].astype(dtype).to_numpy(), df["notch_large_D2"].astype(dtype).to_numpy()]).T
    data["soffset"] = np.array([df["offset_small_D1"].astype(dtype).to_numpy(), df["offset_small_D2"].astype(dtype).to_numpy()]).T
    data["loffset"] = np.array([df["offset_large_D1"].astype(dtype).to_numpy(), df["offset_large_D2"].astype(dtype).to_numpy()]).T
    return data


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
        if k not in params.columns:
            raise ValueError(f"Missing filter parameter {k} in mark_focused")
    # Make sure params have 0-based indexing
    params = params.reset_index(drop=True)

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
    width = params.at[0, "width"]
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


def mark_focused_fast(df, params, inplace=False):
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
        if k not in params.columns:
            raise ValueError(f"Missing filter parameter {k} in mark_focused")
    # Make sure params have 0-based indexing
    params = params.reset_index(drop=True)

    if not inplace:
        df = df.copy()

    # Filter for aligned/focused particles
    #
    # Filter aligned particles (D1 = D2), with correction for D1 D2
    # sensitivity difference. Assume width is same for all quantiles so just
    # grab first width value and calculate aligned particles once
    d1, d2, fsc = evt_as_np(df).values()
    width, snotch, lnotch, soffset, loffset = params_as_np(params).values()
    res = filter_np_jit(d1, d2, fsc, width[0], snotch, lnotch, soffset, loffset)
    df["noise"] = res[0]
    df["saturated"] = res[1]
    df["q2.5"] = res[2]
    df["q50"] = res[3]
    df["q97.5"] = res[4]
    return df


@numba.jit(nopython=True, fastmath=False, parallel=False)
def filter_np_jit(
        d1: npt.NDArray[np.float32],
        d2: npt.NDArray[np.float32],
        fsc: npt.NDArray[np.float32],
        width: np.float32,
        snotch: npt.NDArray[np.float32],
        lnotch: npt.NDArray[np.float32],
        soffset: npt.NDArray[np.float32],
        loffset: npt.NDArray[np.float32]
    ) -> list[npt.NDArray[np.bool_]]:
    if fsc.shape[0] == 0:
        return [
            np.zeros(0, dtype=np.bool_),
            np.zeros(0, dtype=np.bool_),
            np.zeros(0, dtype=np.bool_),
            np.zeros(0, dtype=np.bool_),
            np.zeros(0, dtype=np.bool_)
        ]
    noise = ~((fsc > 1) | (d1 > 1) | (d2 > 1))
    sat = (d1 == np.max(d1)) | (d2 == np.max(d2))
    aligned = ~noise & ~sat & (d1 < d2 + width) & (d2 < d1 + width)

    # noise, sat, q2.5, q50, q97.5
    results = [noise, sat]
    i = 0
    while i < 3:
        results.append(
            aligned &
            (
                ((d1 <= ((fsc * snotch[i, 0]) + soffset[i, 0])) & (d2 <= ((fsc * snotch[i, 1]) + soffset[i, 1]))) | 
                ((d1 <= ((fsc * lnotch[i, 0]) + loffset[i, 0])) & (d2 <= ((fsc * lnotch[i, 1]) + loffset[i, 1])))
            )
        )
        i += 1
    return results


def mark_noise(df):
    """
    Mark data below noise threshold.

    This function returns a boolean Series marking events where none of
    D1, D2, or fsc_small are > 1.

    Returns
    -------
    numpy.ndarray
        Boolean array of noise events.
    """
    if len(set(list(df)).intersection(set(["D1", "D2", "fsc_small"]))) < 3:
        raise ValueError("Can't apply noise filter without D1, D2, and fsc_small")

    # Mark noise events in new column "noise"
    return ~((df["fsc_small"].values > 1) | (df["D1"].values > 1) | (df["D2"].values > 1))


def mark_saturated(df, cols=None):
    """
    Mark data that saturates D1 or D2.

    This function returns a boolean Series marking events where
    D1 == max(D1) or D2 == max(D2).

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow EVT data.
    cols: List of str, , default ["D1", "D2"]
        Columns to test for saturation.

    Returns
    -------
    numpy.ndarray
        Boolean array of saturated events.
    """
    if cols is None:
        cols = ["D1", "D2"]
    if len(set(list(df)).intersection(set(cols))) < len(cols):
        raise ValueError("Some columns requested are not present in df")
    if len(df.index) == 0:
        return np.full(len(df.index), False)
    else:
        idx = np.zeros(len(df.index), dtype=np.bool_)
        for col in cols:
            idx = (idx | (df[col].values == df[col].values.max()))
        return idx


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
        return df

    # Mark noise and saturated particles
    noise = mark_noise(df)
    sat = mark_saturated(df)

    if np.sum(~noise & ~sat) == 0:
        # All data is noise/saturation filtered
        return df[0:0].copy()

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


def linearize_particles(df, columns):
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
    columns: list of str
        Names of columns to linearize.

    Returns
    -------
    pandas.DataFrame
        Copy of df with linearized values.
    """
    events = df.copy()
    if len(events.index) > 0:
        events[columns] = 10**((events[columns] / 2**16) * 3.5)
    return events


def log_particles(df, columns):
    """
    Opposite of linearize_particles().

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    columns: list of str
        Names of columns to log.

    Returns
    -------
    pandas.DataFrame
        Copy of df with logged values.
    """
    events = df.copy()
    if len(events.index) > 0:
        events[columns] = (np.log10(events[columns]) / 3.5) * 2**16
        events[columns] = events[columns].round(0)
    return events
