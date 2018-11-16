import pandas as pd
from . import util


# Data columns in raw SeaFlow particle DataFrame
columns = [
    "time", "pulse_width", "D1", "D2", "fsc_small", "fsc_perp", "fsc_big",
    "pe", "chl_small", "chl_big"
]
channel_columns = columns[2:]  # flow cytometer channel data columns


def empty_df():
    """
    Create an empty SeaFlow particle DataFrame.

    Returns
    -------
    pandas.DataFrame
    """
    return pd.DataFrame(dtype=float, columns=columns)


def mark_focused(df, params):
    """
    Mark focused particle data.

    Adds two boolean columns to the original DataFrame: "noise" identifies
    events below the instrument noise floor, and "focused" identifies focused
    particles.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event DataFrame.
    params: pandas.DataFrame
        Filtering parameters as pandas DataFrame.
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
            raise ValueError(f"Missing filter parameter {k} in EVT.filter()")

    # Apply noise filter
    mark_noise(df)

    # Filter for aligned/focused particles
    for q in params["quantile"].sort_values():
        p = params[params["quantile"] == q].iloc[0]  # get first row of dataframe as series
        colname = f"q{q}"
        df[colname] = False  # all particles are out of focus until shown otherwise
        if len(df[~df["noise"]].index) > 0:
            # Filter aligned particles (D1 = D2), with correction for D1 D2
            # sensitivity difference
            alignedD1 = ~df["noise"] & (df["D1"] < (df["D2"] + p["width"]))
            alignedD2 = ~df["noise"] & (df["D2"] < (df["D1"] + p["width"]))
            aligned = df[alignedD1 & alignedD2]

            # Filter focused particles
            opp_small_D1 = aligned["D1"] <= ((aligned["fsc_small"] * p["notch_small_D1"]) + p["offset_small_D1"])
            opp_small_D2 = aligned["D2"] <= ((aligned["fsc_small"] * p["notch_small_D2"]) + p["offset_small_D2"])
            opp_large_D1 = aligned["D1"] <= ((aligned["fsc_small"] * p["notch_large_D1"]) + p["offset_large_D1"])
            opp_large_D2 = aligned["D2"] <= ((aligned["fsc_small"] * p["notch_large_D2"]) + p["offset_large_D2"])
            opp_df = aligned[(opp_small_D1 & opp_small_D2) | (opp_large_D1 & opp_large_D2)]

            # Mark focused particles
            df.loc[opp_df.index, colname] = True


def mark_noise(df):
    """
    Mark data below noise threshold.

    This function adds a new boolean column "noise" to the particle DataFrame,
    marking events where none of D1, D2, or fsc_small are > 1.

    Parameters
    ----------
    df: pandas.DataFrame
        SeaFlow event data.
    """
    if len(set(list(df)).intersection(set(["D1", "D2", "fsc_small"]))) < 3:
        raise ValueError("Can't apply noise filter without D1, D2, and fsc_small")
    # Mark noise events in new column "noise"
    signal_selector = (df["fsc_small"] > 1) | (df["D1"] > 1) | (df["D2"] > 1)
    df["noise"] = ~signal_selector


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
        Subset of df where each row is focused in at least on quantile.
    """
    selector = False
    for qcolumn in [c for c in df.columns if c.startswith("q")]:
        selector = selector | df[qcolumn]
    return df[selector]


def transform_particles(df, columns=channel_columns, inplace=True):
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
    inplace: bool, default True
        Modify event DataFrame in-place.

    Returns
    -------
    pandas.DataFrame
        Original event DataFrame or a copy with transformed values.
    """
    if inplace:
        events = df
    else:
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
