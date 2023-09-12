import pandas as pd
from .time import seaflow_rfc3339


def condense_plan(df: pd.DataFrame) -> pd.DataFrame:
    """Create a condensed plan dataframe from per-date ids in date

    Parameters
    ----------
    df: pd.DataFrame
        Should contain columns "date" (with str timestamp or datetime-like
        dtype), and "id", which is often a filter_id or gating_id.

    Returns
    -------
    pd.DataFrame
        Dataframe of "start_date" (as SeaFlow-style string timestamp) and "id".
        Contiguous runs of "id" by time found in df will be condensed to just
        their start date.
    """
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by="date", ignore_index=True).reset_index(drop=True)
    df["id_prev"] = df["id"].shift()
    df["id_ne_prev"] = df["id"] != df["id_prev"]
    df.loc[0, "id_ne_prev"] = True
    cond = df.loc[df["id_ne_prev"], ["date", "id"]]
    cond["date"] = cond["date"].map(seaflow_rfc3339)
    cond = cond.rename(columns={"date": "start_date"})
    cond = cond.reset_index(drop=True)
    return cond
