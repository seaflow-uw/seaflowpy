from __future__ import absolute_import
from builtins import str
import pandas as pd
import sqlite3
import uuid
from . import util
from collections import OrderedDict


def ensure_tables(dbpath):
    """Ensure all popcycle tables exists."""
    con = sqlite3.connect(dbpath)
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS vct (
    cruise TEXT NOT NULL,
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
    PRIMARY KEY (cruise, file, pop, quantile)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS opp (
    cruise TEXT NOT NULL,
    file TEXT NOT NULL,
    all_count INTEGER NOT NULL,
    opp_count INTEGER NOT NULL,
    evt_count INTEGER NOT NULL,
    opp_evt_ratio REAL NOT NULL,
    filter_id TEXT NOT NULL,
    quantile REAL NOT NULL,
    PRIMARY KEY (cruise, file, filter_id, quantile)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS sfl (
    --First two columns are the SFL composite key
    cruise TEXT NOT NULL,
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
    flow_rate REAL,
    event_rate REAL,
    PRIMARY KEY (cruise, file)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS filter (
    id TEXT NOT NULL,
    date TEXT NOT NULL,
    quantile REAL NOT NULL,
    serial TEXT NOT NULL,
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
    cruise TEXT NOT NULL,
    file TEXT NOT NULL,
    flag INTEGER,
    PRIMARY KEY (cruise, file)
)""")

    cur.execute("""CREATE VIEW IF NOT EXISTS stat AS
    SELECT
        opp.cruise as cruise,
        opp.file as file,
        sfl.date as time,
        sfl.lat as lat,
        sfl.lon as lon,
        opp.opp_evt_ratio as opp_evt_ratio,
        sfl.flow_rate as flow_rate,
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
        opp.cruise == vct.cruise
        AND
        opp.file == vct.file
        AND
        opp.cruise == sfl.cruise
        AND
        opp.file == sfl.file
    ORDER BY
        cruise, time, pop ASC
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


def save_opp_stats(dbpath, vals):
    # NOTE: values inserted must be in the same order as fields in opp
    # table. Defining that order in a list here makes it easier to verify
    # that the right order is used.
    field_order = [
        "cruise",
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
    sql_insert = "INSERT INTO opp VALUES ({})".format(values_str)
    execute(dbpath, sql_insert, vals)


def save_sfl(dbpath, vals):
    # NOTE: values inserted must be in the same order as fields in sfl
    # table. Defining that order in a list here makes it easier to verify
    # that the right order is used.
    field_order = [
        "cruise",
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
        "flow_rate",
        "event_rate"
    ]
    sql_delete = "DELETE FROM sfl WHERE cruise == :cruise AND file == :file"
    executemany(dbpath, sql_delete, vals)

    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT INTO sfl VALUES (%s)" % values_str
    executemany(dbpath, sql_insert, vals)


def get_filter_table(dbpath):
    sql = "SELECT * FROM filter ORDER BY date ASC, quantile ASC"
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = pd.read_sql(sql, dbcon)
    return filterdf


def get_latest_filter(dbpath):
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = pd.read_sql("SELECT * FROM filter ORDER BY date DESC, quantile ASC", dbcon)
        _id = filterdf.iloc[0]["id"]
    return filterdf[filterdf["id"] == _id]


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
