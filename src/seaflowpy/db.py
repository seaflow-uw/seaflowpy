from builtins import str
from . import errors
import arrow
import pandas as pd
import sqlite3
import uuid

from collections import OrderedDict


def ensure_tables(dbpath):
    """Ensure all popcycle tables exists."""
    con = sqlite3.connect(dbpath)
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS metadata (
    cruise TEXT NOT NULL,
    inst TEXT NOT NULL,
    PRIMARY KEY (cruise, inst)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS vct (
    file TEXT NOT NULL,
    pop TEXT NOT NULL,
    count INTEGER NOT NULL,
    D1_mean REAL NOT NULL,
    D1_min REAL NOT NULL,
    D1_max REAL NOT NULL,
    D2_mean REAL NOT NULL,
    D2_min REAL NOT NULL,
    D2_max REAL NOT NULL,
    fsc_small_mean REAL NOT NULL,
    fsc_small_min REAL NOT NULL,
    fsc_small_max REAL NOT NULL,
    chl_small_mean REAL NOT NULL,
    chl_small_min REAL NOT NULL,
    chl_small_max REAL NOT NULL,
    pe_mean REAL NOT NULL,
    pe_min REAL NOT NULL,
    pe_max REAL NOT NULL,
    fsc_perp_mean REAL NOT NULL,
    fsc_perp_min REAL NOT NULL,
    fsc_perp_max REAL NOT NULL,
    gating_id TEXT NOT NULL,
    filter_id TEXT NOT NULL,
    quantile REAL NOT NULL,
    PRIMARY KEY (file, pop, quantile)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS opp (
    file TEXT NOT NULL,
    all_count INTEGER NOT NULL,
    opp_count INTEGER NOT NULL,
    evt_count INTEGER NOT NULL,
    opp_evt_ratio REAL NOT NULL,
    filter_id TEXT NOT NULL,
    quantile REAL NOT NULL,
    PRIMARY KEY (file, filter_id, quantile)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS sfl (
    file TEXT NOT NULL,
    date TEXT,
    file_duration REAL,
    lat REAL,
    lon REAL,
    conductivity REAL,
    salinity REAL,
    ocean_tmp REAL,
    par REAL,
    bulk_red REAL,
    stream_pressure REAL,
    event_rate REAL,
    PRIMARY KEY (file)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS filter (
    id TEXT NOT NULL,
    date TEXT NOT NULL,
    quantile REAL NOT NULL,
    beads_fsc_small REAL NOT NULL,
    beads_D1 REAL NOT NULL,
    beads_D2 REAL NOT NULL,
    width REAL NOT NULL,
    notch_small_D1 REAL NOT NULL,
    notch_small_D2 REAL NOT NULL,
    notch_large_D1 REAL NOT NULL,
    notch_large_D2 REAL NOT NULL,
    offset_small_D1 REAL NOT NULL,
    offset_small_D2 REAL NOT NULL,
    offset_large_D1 REAL NOT NULL,
    offset_large_D2 REAL NOT NULL,
    PRIMARY KEY (id, quantile)
)""")


    cur.execute("""CREATE TABLE IF NOT EXISTS gating (
    id TEXT NOT NULL,
    date TEXT NOT NULL,
    pop_order INTEGER NOT NULL,
    pop TEXT NOT NULL,
    method TEXT NOT NULL,
    channel1 TEXT,
    channel2 TEXT,
    gate1 REAL,
    gate2 REAL,
    position1 INTEGER,
    position2 INTEGER,
    scale REAL,
    minpe REAL,
    PRIMARY KEY (id, pop)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS poly (
    pop TEXT NOT NULL,
    fsc_small REAL,
    fsc_perp REAL,
    fsc_big REAL,
    pe REAL,
    chl_small REAL,
    chl_big REAL,
    point_order INTEGER NOT NULL,
    gating_id TEXT NOT NULL
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS outlier (
    file TEXT NOT NULL,
    flag INTEGER,
    PRIMARY KEY (file)
)""")

    cur.execute("""CREATE VIEW IF NOT EXISTS stat AS
    SELECT
        opp.file as file,
        sfl.date as time,
        sfl.lat as lat,
        sfl.lon as lon,
        opp.opp_evt_ratio as opp_evt_ratio,
        sfl.file_duration as file_duration,
        vct.pop as pop,
        vct.count as n_count,
        vct.count / (sfl.flow_rate * (sfl.file_duration/60) * opp.opp_evt_ratio) as abundance,
        vct.fsc_small as fsc_small,
        vct.chl_small as chl_small,
        vct.pe as pe
    FROM
        opp, vct, sfl
    WHERE
        opp.filter_id == (select id FROM filter ORDER BY date DESC limit 1)
        AND
        opp.file == vct.file
        AND
        opp.file == sfl.file
    ORDER BY
        time, pop ASC
""")

    con.commit()
    con.close()


def ensure_indexes(dbpath):
    """Create table indexes."""
    con = sqlite3.connect(dbpath)
    cur = con.cursor()
    index_cmds = [
        "CREATE INDEX IF NOT EXISTS oppFileIndex ON opp (file)",
        "CREATE INDEX IF NOT EXISTS vctFileIndex ON vct (file)",
        "CREATE INDEX IF NOT EXISTS sflDateIndex ON sfl (date)",
        "CREATE INDEX IF NOT EXISTS outlierFileIndex ON outlier (file)"
    ]
    for cmd in index_cmds:
        cur.execute(cmd)
    con.commit()
    con.close()


def create_db(dbpath):
    """Create or complete database"""
    ensure_tables(dbpath)
    ensure_indexes(dbpath)


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


def save_opp_stats(dbpath, vals):
    create_db(dbpath)
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
    # Construct values string with named placeholders
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT OR REPLACE INTO opp VALUES ({})".format(values_str)
    execute(dbpath, sql_insert, vals)


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


def execute(dbpath, sql, values=None, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    if values is not None:
        con.execute(sql, values)
    else:
        con.execute(sql)
    con.commit()
    con.close()


def executemany(dbpath, sql, values=None, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    con.executemany(sql, values)
    con.commit()
    con.close()
