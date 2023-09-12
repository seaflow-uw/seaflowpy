from builtins import str
import datetime
import pkgutil
import sqlite3
import uuid
from pathlib import Path
import pandas as pd
from . import errors
from . import particleops
from . import plan
from .seaflowfile import SeaFlowFile


def create_db(dbpath):
    """Create or complete database"""
    schema_text = pkgutil.get_data(__name__, 'data/popcycle.sql').decode('UTF-8', 'ignore')
    Path(dbpath).parent.mkdir(parents=True, exist_ok=True)
    executescript(dbpath, schema_text)


def save_filter_params(dbpath, vals):
    create_db(dbpath)
    # NOTE: values inserted must be in the same order as fields in opp
    # table. Defining that order in a list here makes it easier to verify
    # that the right order is used.
    field_order = [
        "id",
        "date",
        "quantile",
        "beads_fsc_small",
        "beads_D1",
        "beads_D2",
        "width",
        "notch_small_D1",
        "notch_small_D2",
        "notch_large_D1",
        "notch_large_D2",
        "offset_small_D1",
        "offset_small_D2",
        "offset_large_D1",
        "offset_large_D2"
    ]
    # Construct values string with named placeholders
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO filter VALUES ({})".format(values_str)
    id_ = str(uuid.uuid4())
    # Create an ISO8601 timestamp in the format '2019-04-29T20:54:26+00:00'
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    date = utcnow.isoformat(timespec='seconds')
    for v in vals:
        v['id'] = id_
        v['date'] = date
    executemany(dbpath, sql_insert, vals)


def save_df(dbpath, table, df, delete_first=True):
    """Save dataframe to db table

    If delete_first is true, delete entries in table first without dropping and
    recreating the table.
    
    The table schema should stay consistent with the schema at db creation after
    this function runs.
    """
    create_db(dbpath)
    if delete_first:
        execute(dbpath, f"DELETE FROM {table}")

    try:
        with sqlite3.connect(dbpath) as con:
            df.to_sql(table, con, index=False, if_exists="append")
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when saving {!s} table: {!s}".format(table, e))


def save_metadata(dbpath, vals):
    create_db(dbpath)
    # Bit drastic but there should only be one entry in metadata at a time
    sql_delete = "DELETE FROM metadata"
    execute(dbpath, sql_delete)

    sql_insert = "INSERT INTO metadata VALUES (:cruise, :inst)"
    executemany(dbpath, sql_insert, vals)


def save_opp_to_db(vals, dbpath):
    """
    Save aggregate statistics for filtered particle data to SQLite.

    Parameters
    ----------
    vals: list of dicts
        Values array to be saved to opp table, created by prep_opp().
    dbpath: str
        Path to SQLite DB file.
    """
    # NOTE: values inserted must be in the same order as fields in opp
    # table. Defining that order in a list here makes it easier to verify
    # that the right order is used.
    field_order = [
        "file",
        "all_count",
        "opp_count",
        "evt_count",
        "opp_evt_ratio",
        "filter_id",
        "quantile"
    ]
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO opp VALUES ({})".format(values_str)
    executemany(dbpath, sql_insert, vals)


def prep_opp(file, df, all_count, evt_count, filter_id):
    """
    Prepare aggregate statistic values for filtered particle data to SQLite.

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
    Array of values for save_opp_to_db().
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
    return vals


def save_outlier(vals, dbpath):
    """
    Save entries in outlier table.

    Parameters
    ----------
    vals: list of dicts
        Values array to be saved to outlier table, created by prep_outlier().
    dbpath: str
        Path to SQLite DB file.
    """
    field_order = ["file", "flag"]
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO outlier VALUES ({})".format(values_str)
    executemany(dbpath, sql_insert, vals)


def prep_outlier(file, flag):
    """
    Prepare an outlier entry for this file.

    Parameters
    ----------
    file: str
        Path to SeaFlow file that was filtered. Used to get the canonical
        SeaFlow file ID.
        e.g. tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00 will become
        2014_185/2014-07-04T00-00-02+00-00.
    flag: int
        Outlier table value, 0 for OK.

    Returns
    -------
    A single item array of values for save_outlier().
    """
    return [{"file": SeaFlowFile(file).file_id, "flag": flag}]


def save_sfl(dbpath, vals):
    create_db(dbpath)

    # Remove any previous SFL data
    sql_delete = "DELETE FROM sfl"
    execute(dbpath, sql_delete)

    # NOTE: values inserted must be in the same order as fields in sfl
    # table. Defining that order in a list here makes it easier to verify
    # that the right order is used.
    field_order = [
        "file",
        "date",
        "file_duration",
        "lat",
        "lon",
        "conductivity",
        "salinity",
        "ocean_tmp",
        "par",
        "bulk_red",
        "stream_pressure",
        "event_rate"
    ]
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO sfl VALUES (%s)" % values_str
    executemany(dbpath, sql_insert, vals)


def get_cruise(dbpath):
    sql = "SELECT cruise FROM metadata"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    if len(df.index) > 1:
        cruises = ", ".join([str(c) for c in df.cruise.tolist()])
        raise errors.SeaFlowpyError("More than one cruise found in database {}: {}".format(dbpath, cruises))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No cruise name found in database {}\n".format(dbpath))
    return df.cruise.tolist()[0]


def get_filter_table(dbpath):
    sql = "SELECT * FROM filter ORDER BY date ASC, quantile ASC"
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = safe_read_sql(sql, dbcon)
    return filterdf


def get_filter_plan_table(dbpath):
    sql = "SELECT * FROM filter_plan"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_gating_table(dbpath):
    sql = "SELECT * FROM gating ORDER BY date ASC"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_gating_plan_table(dbpath):
    sql = "SELECT * FROM gating_plan ORDER BY start_date ASC"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_poly_table(dbpath):
    sql = "SELECT * FROM poly ORDER BY gating_id, pop, point_order ASC"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_serial(dbpath):
    sql = "SELECT inst FROM metadata"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    if len(df.index) > 1:
        insts = ", ".join([str(c) for c in df.inst.tolist()])
        raise errors.SeaFlowpyError("More than one instrument serial found in database {}: {}".format(dbpath, insts))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No instrument serial found in database {}\n".format(dbpath))
    return df.inst.tolist()[0]


def get_latest_filter(dbpath):
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbcon)
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No filter parameters found in database {}\n".format(dbpath))
    _id = df.iloc[0]["id"]
    return df[df["id"] == _id]


def get_filter_params_lookup(dbpath, files_df):
    files_df = files_df.copy().reset_index(drop=False)
    with sqlite3.connect(dbpath) as dbcon:
        filter_df = safe_read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbcon)
        filter_plan_df = safe_read_sql("SELECT * FROM filter_plan ORDER BY start_date ASC", dbcon)
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
    with sqlite3.connect(dbpath) as dbcon:
        oppdf = safe_read_sql(sql, dbcon)
    return oppdf


def get_vct_table(dbpath):
    """Get vct table joined to SFL to add a date column"""
    sql = "SELECT vct.*, sfl.date FROM vct INNER JOIN sfl ON vct.file = sfl.file ORDER BY file ASC, pop ASC, quantile ASC"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_outlier_table(dbpath):
    sql = "SELECT * FROM outlier ORDER BY file ASC"
    with sqlite3.connect(dbpath) as dbcon:
        outlierdf = safe_read_sql(sql, dbcon)
    return outlierdf


def get_sfl_table(dbpath):
    sql = "SELECT * FROM sfl ORDER BY date ASC"
    with sqlite3.connect(dbpath) as dbcon:
        df = safe_read_sql(sql, dbcon)
    return df


def get_event_counts(dbpath):
    filterid = get_latest_filter(dbpath).loc[0, "id"]
    opp = get_opp_table(dbpath, filterid)
    grouped = opp[["file", "all_count"]].groupby(["file"])
    return {name: group["all_count"].head(1).values[0] for name, group in grouped}


def merge_dbs(db1, db2):
    """Merge two SQLite databases into a new database."""
    with sqlite3.connect(db1) as con1:
        with sqlite3.connect(db2) as con2:
            gatingdf = safe_read_sql('select * from gating', con1)
            polydf = safe_read_sql('select * from poly', con1)
            filterdf = safe_read_sql('select * from filter', con1)
            gatingdf.to_sql('gating', con2, if_exists='append', index=False)
            polydf.to_sql('poly', con2, if_exists='append', index=False)
            filterdf.to_sql('filter', con2, if_exists='append', index=False)
    # Merge opp
    # Merge vct
    # Merge meta


def create_filter_plan(dbpath):
    """
    Create a filter plan table.

    Only run for the simple case where sfl and filter tables are populated and
    there is only one set of filter parameters.

    Raise SeaFlowpyError if filter or sfl tables are empty, if more than one set
    of filter parameters exists in the database, if a filter plan already
    exists, or for database save errors.

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
    })
    try:
        with sqlite3.connect(dbpath) as con:
            df_sql_insert(filter_plan_df, "filter_plan", con)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when saving a filter plan: {!s}".format(e))

    return filter_plan_df


def create_gating_plan_from_vct(dbpath):
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


def execute(dbpath, sql, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    try:
        with con:
            con.execute(sql)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when executing SQL queries: {!s}".format(e))
    finally:
        con.close()


def executemany(dbpath, sql, values=None, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    try:
        with con:
            con.executemany(sql, values)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when executing SQL queries: {!s}".format(e))
    finally:
        con.close()

def executescript(dbpath, sql_script_text, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    try:
        with con:
            con.executescript(sql_script_text)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when executing a SQL script: {!s}".format(e))
    finally:
        con.close()


def safe_read_sql(sql, con):
    """Catch and handle error if table not present during pandas.read_sql()"""
    try:
        df = pd.read_sql(sql, con)
        errmsg = ''
    except pd.io.sql.DatabaseError as e:
        errmsg = str(e)
    if errmsg:
        raise errors.SeaFlowpyError(errmsg)
    return df


def df_sql_insert(df: pd.DataFrame, table: str, con: sqlite3.Connection):
    """Insert from df into SQL table without replacing the table schema"""
    values_str = ", ".join([":" + f for f in df.columns])
    sql_insert = f"INSERT OR REPLACE INTO {table} VALUES ({values_str})"
    values = df.to_dict("index").values()
    con.executemany(sql_insert, values)
