from builtins import str
import datetime
import pkgutil
import sqlite3
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Union
import numpy as np
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine, MetaData, or_, Table
from sqlalchemy.exc import ArgumentError, NoSuchTableError, OperationalError
from . import errors
from . import particleops
from . import plan
from .seaflowfile import SeaFlowFile
from . import sfl


def dbpath_to_url(dbpath: Union[str, Path]) -> str:
    """Normalize a dbpath to SQLAlchemy sqlite3 DB URL"""
    if Path(dbpath).exists():
        dbpath = f"sqlite:///{dbpath}"
    elif not str(dbpath).startswith("sqlite:///"):
        raise ValueError(f"dbpath '{dbpath}' must a path to an existing file or a sqlalchmey sqlite3 URL string")
    return str(dbpath)


def table_cols(table: str, dbpath: Union[str, Path]) -> list[str]:
    """Get column names for table in dbpath"""
    engine = create_engine(f"{dbpath_to_url(dbpath)}")
    sfl_table = Table(table, MetaData(), autoload_with=engine)
    names = [c.name for c in sfl_table.columns]
    engine.dispose()
    return names


def read_table(table: str, dbpath: Union[str, Path]) -> pd.DataFrame:
    """Read table as dataframe sorted ASC by ROWID"""
    try:
        return pd.read_sql(
            f"SELECT * FROM {table} ORDER BY ROWID",
            dbpath_to_url(dbpath),
            dtype_backend="pyarrow"
        )
    except pd.io.sql.DatabaseError as e:
        raise errors.SeaFlowpyError(e) from e


def read_sql(sql: str, dbpath: Union[str, Path]):
    """Read from Catch and handle error if table not present during pandas.read_sql()"""
    try:
        return pd.read_sql(sql, dbpath_to_url(dbpath), dtype_backend="pyarrow")
    except (pd.io.sql.DatabaseError, OperationalError, NoSuchTableError) as e:
        raise errors.SeaFlowpyError(e) from e


def save_df(
    df: pd.DataFrame,
    table: str,
    dbpath: Union[str, Path],
    clear: bool=True,
    replace_by_file: bool=True
):
    """Save dataframe to db table

    If clear, clear entries in table first without dropping and recreating the
    table. If replace_by_file, entries matched by file will be replaced in
    db.
    """
    create_db(dbpath)

    try:
        engine = create_engine(dbpath_to_url(dbpath))
    except ArgumentError as e:
        raise errors.SeaFlowpyError(f"error opening database: {e}") from e

    try:
        with engine.connect() as conn:
            with conn.begin():
                del_stmt = None
                if clear or replace_by_file:
                    table_obj = Table(table, MetaData(), autoload_with=conn)
                    if clear:
                        del_stmt = table_obj.delete().where()
                    elif replace_by_file:
                        if "file" in [c.name for c in table_obj.columns] and "file" in df.columns:
                            file_selections = []
                            for f in df["file"].to_list():
                                file_selections.append(table_obj.c.file == f)
                            del_stmt = table_obj.delete().where(or_(*file_selections))
                    if del_stmt is not None:
                        conn.execute(del_stmt)
                df.to_sql(table, conn, index=False, if_exists="append")
    except (NoSuchTableError, pd.io.sql.DatabaseError) as e:
        raise errors.SeaFlowpyError(f"error saving dataframe to db: {e}") from e
    finally:
        engine.dispose()


def create_db(dbpath):
    """Create or complete database"""
    schema_text = pkgutil.get_data(__name__, 'data/popcycle.sql').decode('UTF-8', 'ignore')
    Path(dbpath).parent.mkdir(parents=True, exist_ok=True)
    executescript(dbpath, schema_text)


def executescript(dbpath, sql_script_text, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    try:
        with con:
            con.executescript(sql_script_text)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when executing a SQL script: {!s}".format(e))
    finally:
        con.close()


def save_filter_params(
    df: pd.DataFrame,
    dbpath: Union[str, Path],
    clear: bool=True
) -> str:
    """Save filter parameters and return the generated filter ID"""
    create_db(dbpath)
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    date = utcnow.isoformat(timespec='seconds')
    id_ = str(uuid.uuid4())
    df.insert(0, "date", date)
    df.insert(0, "id", id_)
    save_df(df, "filter", dbpath, clear=clear)
    return id_


def import_filter_params(
    filter_path: Union[str, Path],
    dbpath: Union[str, Path],
    plan: bool=True,
    clear: bool=False
) -> str:
    types = defaultdict(
        lambda: "float64[pyarrow]",
        cruise=pd.ArrowDtype(pa.string()),
        instrument=pd.ArrowDtype(pa.string())
    )
    df = pd.read_csv(filter_path, dtype=types, dtype_backend="pyarrow")
    df.columns = [c.replace('.', '_') for c in df.columns]
    cruise = get_cruise(dbpath)
    df = df[df.cruise == cruise]
    if len(df) == 0:
        raise errors.SeaFlowpyError('no filter parameters found for cruise %s' % cruise)
    df = df.drop(columns=["instrument", "cruise"])
    id_ = save_filter_params(df, dbpath, clear=clear)
    if plan:
        save_df(create_filter_plan(dbpath), "filter_plan", dbpath, clear=True)
    return id_


def import_gating_params(
    gating_path: Union[str, Path],
    poly_path: Union[str, Path],
    gating_plan_path: Union[str, Path],
    dbpath: Union[str, Path]
):
    gating_df = pd.read_csv(gating_path, sep="\t", dtype_backend="pyarrow")
    poly_df = pd.read_csv(poly_path, sep="\t", dtype_backend="pyarrow")
    gating_plan_df = pd.read_csv(gating_plan_path, sep="\t", dtype_backend="pyarrow", dtype="string")

    save_df(gating_df, "gating", dbpath, clear=True)
    save_df(poly_df, "poly", dbpath, clear=True)
    save_df(gating_plan_df, "gating_plan", dbpath, clear=True)


def import_sfl(
    sfl_path: Union[str, Path],
    dbpath: Union[str, Path],
    force: bool=False
) -> list[dict[str, Any]]:
    """Import SFL file to database

    If there are errors during SLF validation, return the errors without
    altering the database. If there are errors and force is True, save the SFL
    and return errors.

    This function will raise SeaFlowpyError if cruise and instrument serial in
    the metadata table don't match cruise and serial parsed from the SFL file
    name. If the file name does not contain a cruise and serial,
    SeaFlowpyError is raised if they are not present in the metadata table.
    """
    cruise, serial = None, None

    # Try to read cruise and serial from database
    if Path(dbpath).exists():
        try:
            cruise = get_cruise(dbpath)
        except errors.SeaFlowpyError as e:
            pass
        try:
            serial = get_serial(dbpath)
        except errors.SeaFlowpyError as e:
            pass

    # Try to read cruise and serial from filename if not already defined
    file_cruise = None
    file_serial = None
    results = sfl.parse_sfl_filename(sfl_path)
    if results:
        file_cruise = results[0]
        file_serial = results[1]
    if cruise and file_cruise:
        if cruise != file_cruise:
            raise  errors.SeaFlowpyError(
                "cruise from metadata table and file name don't match, make sure filename only has '_' if cruise/serial parsing is desired"
            )
    if serial and file_serial:
        if serial != file_serial:
            raise  errors.SeaFlowpyError(
                "serial from metadata table and file name don't match, , make sure filename only has '_' if cruise/serial parsing is desired"
            )
    # Make sure cruise and serial are defined somewhere
    cruise = cruise or file_cruise
    serial = serial or file_serial
    if cruise is None or serial is None:
        raise errors.SeaFlowpyError(
            'serial and cruise must be in either file name as <cruise>_<serial>.sfl or in db metadata table.'
        )

    # Perform checks on original string data to discriminate between missing
    # data and values that could not be intepreted as numbers
    df = sfl.read_file(sfl_path, convert_numerics=False)
    check_errors = sfl.check(df)

    if force or len([e for e in check_errors if e["level"] == "error"]) == 0:
        # Read SFL again, this time converting all numeric columns when reading file
        sfl.read_file(sfl_path, convert_numerics=True)
        save_df(pd.DataFrame({'cruise': [cruise], 'inst': [serial]}), "metadata", dbpath, clear=True)
        save_sfl(df, dbpath)

    return check_errors


def import_outlier(outlier_path: Union[str, Path], dbpath: Union[str, Path]):
    df = pd.read_csv(outlier_path, sep="\t", dtype_backend="pyarrow")
    save_df(df, "outlier", dbpath, clear=True)


def export_gating_params(dbpath: Union[str, Path], out_prefix: Union[str, Path]):
    gating_df = get_gating_table(dbpath)
    poly_df = get_poly_table(dbpath)
    try:
        gating_plan_df = get_gating_plan_table(dbpath)
    except errors.SeaFlowpyError as e:
        # Maybe this is an older db schema without gating_plan table
        gating_plan_df = None

    if gating_plan_df is None or len(gating_plan_df) == 0:
        gating_plan_df = create_gating_plan(dbpath)
        if len(gating_plan_df) == 0:
            raise errors.SeaFlowpyError("could not create gating_plan from db")

    Path(out_prefix).parent.mkdir(exist_ok=True, parents=True)
    gating_df.to_csv(f"{out_prefix}.gating.tsv", sep="\t", index=False)
    poly_df.to_csv(f"{out_prefix}.poly.tsv", sep="\t", index=False)
    gating_plan_df.to_csv(f"{out_prefix}.gating_plan.tsv", sep="\t", index=False)


def export_outlier(
    dbpath: Union[str, Path],
    out_path: Union[str, Path],
    populated: bool=True
):
    outlier_df = get_outlier_table(dbpath)
    if populated and np.all(outlier_df["flag"] == 0):
        return
    Path(out_path).parent.mkdir(exist_ok=True, parents=True)
    outlier_df.to_csv(out_path, sep="\t", index=False)


def save_opp_to_db(df: pd.DataFrame, dbpath: Union[str, Path]):
    """Save aggregate statistics for filtered particle data to SQLite"""
    save_df(df, "opp", dbpath, clear=False)


def prep_opp(
    file: str,
    df: pd.DataFrame,
    all_count: int,
    evt_count: int,
    filter_id: str
) -> pd.DataFrame:
    """Prepare aggregate statistics values for filtered particle data

    The array returned by this function can be passed to save_opp_to_db.

    Parameters
    ----------
    file: str
        Path to SeaFlow file that was filtered. Used to get the canonical
        SeaFlow file ID.
        e.g. tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00 will become
        2014_185/2014-07-04T00-00-02+00-00.
    df: pandas.DataFrame
        SeaFlow particle data. Focused particle flag columns for each quantile
        should be in columns "q<quantile>" e.g. q2.5 for the 2.5 quantile.
    all_count: int
        Event count in raw file.
    evt_count: int
        Events above noise floor in raw file.
    filter_id: str
        DB ID for filtering parameters used to create OPP.

    Returns
    -------
    DataFrame of opp aggregate statistics matching opp table structure
    """
    vals = []
    for _q_col, q, _q_str, q_df in particleops.quantiles_in_df(df):
        opp_count = len(q_df.index)
        try:
            opp_evt_ratio = opp_count / evt_count
        except ZeroDivisionError:
            opp_evt_ratio = 0.0
        vals.append({
            "file": SeaFlowFile(file).file_id,
            "all_count": all_count,
            "opp_count": opp_count,
            "evt_count": evt_count,
            "opp_evt_ratio": opp_evt_ratio,
            "filter_id": filter_id,
            "quantile": q
        })
    df = pd.DataFrame(vals)
    return df


def save_sfl(df: pd.DataFrame, dbpath: Union[str, Path]):
    create_db(dbpath)
    cols = table_cols("sfl", dbpath)
    save_df(df[cols], "sfl", dbpath, clear=True)


def get_cruise(dbpath):
    sql = "SELECT cruise FROM metadata"
    df = read_sql(sql, dbpath)
    if len(df.index) > 1:
        cruises = ", ".join([str(c) for c in df.cruise.tolist()])
        raise errors.SeaFlowpyError("More than one cruise found in database {}: {}".format(dbpath, cruises))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No cruise name found in database {}\n".format(dbpath))
    return df.cruise.tolist()[0]


def get_filter_table(dbpath):
    sql = "SELECT * FROM filter ORDER BY date ASC, quantile ASC"
    return read_sql(sql, dbpath)


def get_filter_plan_table(dbpath):
    sql = "SELECT * FROM filter_plan ORDER BY start_date ASC"
    return read_sql(sql, dbpath)


def get_gating_table(dbpath):
    sql = "SELECT * FROM gating ORDER BY date ASC"
    return read_sql(sql, dbpath)


def get_gating_plan_table(dbpath):
    sql = "SELECT * FROM gating_plan ORDER BY start_date ASC"
    return read_sql(sql, dbpath)


def get_poly_table(dbpath):
    sql = "SELECT * FROM poly ORDER BY gating_id, pop, point_order ASC"
    return read_sql(sql, dbpath)


def get_serial(dbpath):
    sql = "SELECT inst FROM metadata"
    df = read_sql(sql, dbpath)
    if len(df.index) > 1:
        insts = ", ".join([str(c) for c in df.inst.tolist()])
        raise errors.SeaFlowpyError("More than one instrument serial found in database {}: {}".format(dbpath, insts))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No instrument serial found in database {}\n".format(dbpath))
    return df.inst.tolist()[0]


def get_latest_filter(dbpath):
    df = read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbpath)
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No filter parameters found in database {}\n".format(dbpath))
    _id = df.iloc[0]["id"]
    return df[df["id"] == _id]


def get_filter_params_lookup(dbpath, files_df):
    files_df = files_df.copy().reset_index(drop=False)
    filter_df = read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbpath)
    filter_plan_df = read_sql("SELECT * FROM filter_plan ORDER BY start_date ASC", dbpath)
    if len(filter_df.index) == 0:
        raise errors.SeaFlowpyError("No filter parameters found in database {}\n".format(dbpath))
    if len(filter_plan_df.index) == 0:
        raise errors.SeaFlowpyError("No filter plan found in database {}\n".format(dbpath))

    files_df["filter_id"] = None
    if len(filter_plan_df) > 1:
        for i in range(len(filter_plan_df) - 1):
            gte = files_df["date"] >= filter_plan_df.loc[i, "start_date"]
            lte = files_df["date"] <= filter_plan_df.loc[i + 1, "start_date"]
            files_df.loc[(gte & lte), "filter_id"] = filter_plan_df.loc[i, "filter_id"]
    i = len(filter_plan_df) - 1
    gte = files_df["date"] >= filter_plan_df.loc[i, "start_date"]
    files_df.loc[gte, "filter_id"] = filter_plan_df.loc[i, "filter_id"]

    filter_params = {}
    for i, row in files_df.iterrows():
        filter_params[row["file_id"]] = filter_df[filter_df["id"] == row["filter_id"]].reset_index(drop=True)

    return filter_params


def get_opp_table(dbpath, filter_id=""):
    if filter_id == "":
        sql = "SELECT * FROM opp ORDER BY file ASC, quantile ASC"
    else:
        sql = "SELECT * FROM opp WHERE filter_id = '{}' ORDER BY file ASC, quantile ASC".format(filter_id)
    return read_sql(sql, dbpath)


def get_vct_table(dbpath):
    """Get vct table joined to SFL to add a date column"""
    sql = "SELECT vct.*, sfl.date FROM vct INNER JOIN sfl ON vct.file = sfl.file ORDER BY file ASC, pop ASC, quantile ASC"
    return read_sql(sql, dbpath)


def get_outlier_table(dbpath):
    sql = "SELECT * FROM outlier ORDER BY file ASC"
    return read_sql(sql, dbpath)


def get_sfl_table(dbpath):
    sql = "SELECT * FROM sfl ORDER BY date ASC"
    return read_sql(sql, dbpath)


def get_event_counts(dbpath):
    filterid = get_latest_filter(dbpath).loc[0, "id"]
    opp = get_opp_table(dbpath, filterid)
    grouped = opp[["file", "all_count"]].groupby(["file"])
    return {name: group["all_count"].head(1).values[0] for name, group in grouped}


def create_filter_plan(dbpath: Union[str, Path]) -> pd.DataFrame:
    """
    Return a filter plan dataframe from database contents.

    Only run for the simple case where sfl and filter tables are populated and
    there is only one set of filter parameters.

    Raise SeaFlowpyError if filter or sfl tables are empty, if more than one set
    of filter parameters exists in the database, if a filter plan already
    exists.

    Return a dataframe of the filter plan.
    """
    filter_df = get_filter_table(dbpath)
    sfl_df = get_sfl_table(dbpath)
    cur_filter_plan_df = get_filter_plan_table(dbpath)
    if len(filter_df) == 0:
        raise errors.SeaFlowpyError("no filter parameters found in db")
    if len(filter_df["id"].unique()) > 1:
        raise errors.SeaFlowpyError("more than one filter parameter found in db")
    if len(sfl_df) == 0:
        raise errors.SeaFlowpyError("no sfl data found in db")
    if len(cur_filter_plan_df) > 0:
        raise errors.SeaFlowpyError("a filter plan already exists in db")
    filter_plan_df = pd.DataFrame({
        "start_date": [sfl_df.sort_values(by="date").loc[0, "date"]],
        "filter_id": [filter_df.loc[0, "id"]]
    }, dtype=pd.ArrowDtype(pa.string()))
    return filter_plan_df


def create_gating_plan(dbpath: Union[str, Path]) -> pd.DataFrame:
    """
    Create a gating plan table from vct

    Raise SeaFlowpyError if vct or sfl tables are empty or non-existent.

    Return a dataframe of the gating plan.
    """
    gating_df = get_gating_table(dbpath)
    vct_df = get_vct_table(dbpath)
    vct_df = vct_df[vct_df["quantile"] == 2.5]
    if len(vct_df) == 0:
        raise errors.SeaFlowpyError("no data in vct/sfl tables, can't create a gating_plan")
    # Make sure all gating_ids in vct are in gating, otherwise throw
    vct_ids = set(vct_df["gating_id"].unique())
    gating_ids = set(gating_df["id"].unique())
    if len(vct_ids.difference(gating_ids)) > 0:
        raise errors.SeaFlowpyError("gating IDs found in vct table that are not in gating table")
    vct_df = vct_df.rename(columns={"gating_id": "id"})[["date", "id"]]
    gating_plan_df = plan.condense_plan(vct_df).rename(columns={"id": "gating_id"})
    return gating_plan_df
