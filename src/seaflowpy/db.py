from builtins import str
from . import errors
from . import particleops
from .seaflowfile import SeaFlowFile
from shutil import copyfile
import arrow
import pkg_resources
import pandas as pd
import sqlite3
import uuid

from collections import OrderedDict


def create_db(dbpath):
    """Create or complete database"""
    schema_path = pkg_resources.resource_filename(__name__, 'data/popcycle.sql')
    with open(schema_path, 'r') as fh:
        executescript(dbpath, fh.read())


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
    date = arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZZ')
    for v in vals:
        v['id'] = id_
        v['date'] = date
    executemany(dbpath, sql_insert, vals)


def save_metadata(dbpath, vals):
    create_db(dbpath)
    # Bit drastic but there should only be one entry in metadata at a time
    sql_delete = "DELETE FROM metadata"
    executemany(dbpath, sql_delete, vals)

    sql_insert = "INSERT INTO metadata VALUES (:cruise, :inst)"
    executemany(dbpath, sql_insert, vals)


def save_opp_to_db(file, df, all_count, evt_count, filter_id, dbpath):
    """
    Save aggregate statistics for filtered particle data to SQLite.

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
    vals = []
    for q_col, q, q_str, q_df in particleops.quantiles_in_df(df):
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
    # Construct values string with named placeholders
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO opp VALUES ({})".format(values_str)
    executemany(dbpath, sql_insert, vals)


def save_sfl(dbpath, vals):
    create_db(dbpath)
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
        df = pd.read_sql(sql, dbcon)
    if len(df.index) > 1:
        cruises = ", ".join([str(c) for c in df.cruise.tolist()])
        raise errors.SeaFlowpyError("More than one cruise found in database {}: {}".format(dbpath, cruises))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No cruise name found in database {}\n".format(dbpath))
    return df.cruise.tolist()[0]


def get_filter_table(dbpath):
    sql = "SELECT * FROM filter ORDER BY date ASC, quantile ASC"
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = pd.read_sql(sql, dbcon)
    return filterdf


def get_serial(dbpath):
    sql = "SELECT inst FROM metadata"
    with sqlite3.connect(dbpath) as dbcon:
        df = pd.read_sql(sql, dbcon)
    if len(df.index) > 1:
        insts = ", ".join([str(c) for c in df.inst.tolist()])
        raise errors.SeaFlowpyError("More than one instrument serial found in database {}: {}".format(dbpath, insts))
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No instrument serial found in database {}\n".format(dbpath))
    return df.inst.tolist()[0]


def get_latest_filter(dbpath):
    with sqlite3.connect(dbpath) as dbcon:
        df = pd.read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbcon)
    if len(df.index) == 0:
        raise errors.SeaFlowpyError("No filter parameters found in database {}\n".format(dbpath))
    _id = df.iloc[0]["id"]
    return df[df["id"] == _id]


def get_opp(dbpath, filter_id):
    sql = "SELECT * FROM opp WHERE filter_id = '{}' ORDER BY file ASC, quantile ASC".format(filter_id)
    with sqlite3.connect(dbpath) as dbcon:
        oppdf = pd.read_sql(sql, dbcon)
    return oppdf


def merge_dbs(db1, db2):
    """Merge two SQLite databases into a new database."""
    with sqlite3.connect(db1) as con1:
        with sqlite3.connect(db2) as con2:
            gatingdf = pd.read_sql('select * from gating', con1)
            polydf = pd.read_sql('select * from poly', con1)
            filterdf = pd.read_sql('select * from filter', con1)
            gatingdf.to_sql('gating', con2, if_exists='append', index=False)
            polydf.to_sql('poly', con2, if_exists='append', index=False)
            filterdf.to_sql('filter', con2, if_exists='append', index=False)
    # Merge opp
    # Merge vct
    # Merge meta


def execute(dbpath, sql, values=None, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    try:
        with con:
            if values is not None:
                con.execute(sql, values)
            else:
                con.execute(sql)
    except sqlite3.Error as e:
        raise errors.SeaFlowpyError("An error occurred when executing a SQL query: {!s}".format(e))
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
