import pandas as pd
import sqlite3
import uuid
import util
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
    fsc_small REAL NOT NULL,
    fsc_perp REAL NOT NULL,
    pe REAL NOT NULL,
    chl_small REAL NOT NULL,
    gating_id TEXT NOT NULL,
    PRIMARY KEY (cruise, file, pop, gating_id)
)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS opp (
    cruise TEXT NOT NULL,
    file TEXT NOT NULL,
    all_count INTEGER NOT NULL,
    opp_count INTEGER NOT NULL,
    evt_count INTEGER NOT NULL,
    opp_evt_ratio REAL NOT NULL,
    notch1 REAL NOT NULL,
    notch2 REAL NOT NULL,
    offset REAL NOT NULL,
    origin REAL NOT NULL,
    width REAL NOT NULL,
    fsc_small_min REAL NOT NULL,
    fsc_small_max REAL NOT NULL,
    fsc_small_mean REAL NOT NULL,
    fsc_perp_min REAL NOT NULL,
    fsc_perp_max REAL NOT NULL,
    fsc_perp_mean REAL NOT NULL,
    fsc_big_min REAL NOT NULL,
    fsc_big_max REAL NOT NULL,
    fsc_big_mean REAL NOT NULL,
    pe_min REAL NOT NULL,
    pe_max REAL NOT NULL,
    pe_mean REAL NOT NULL,
    chl_small_min REAL NOT NULL,
    chl_small_max REAL NOT NULL,
    chl_small_mean REAL NOT NULL,
    chl_big_min REAL NOT NULL,
    chl_big_max REAL NOT NULL,
    chl_big_mean REAL NOT NULL,
    filter_id TEXT NOT NULL,
    PRIMARY KEY (cruise, file, filter_id)
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
    notch1 REAL,
    notch2 REAL,
    offset REAL NOT NULL,
    origin REAL,
    width REAL NOT NULL,
    PRIMARY KEY (id)
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
        "CREATE INDEX IF NOT EXISTS sflDateIndex ON sfl (date)"
    ]
    for cmd in index_cmds:
        cur.execute(cmd)
    con.commit()
    con.close()


def save_filter_params(dbpath, filter_options):
    """Save filtering parameters

    Arguments:
        dbpath - SQLite3 database file path
        filter_options - Dictionary of filter params
            (notch1, notch2, width, offset, origin)

    Returns:
        UUID primary key for this entry in filter table
    """
    values = dict(filter_options)  # Make a copy to preserve original
    values["date"] = util.iso8601_now()  # Datestamp for right now
    values["id"] = str(uuid.uuid4())

    values_str = "(:id, :date, :notch1, :notch2, :offset, :origin, :width)"
    sql = "INSERT INTO filter VALUES %s" % values_str
    execute(dbpath, sql, values)
    return values["id"]


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
        "notch1",
        "notch2",
        "offset",
        "origin",
        "width",
        "fsc_small_min",
        "fsc_small_max",
        "fsc_small_mean",
        "fsc_perp_min",
        "fsc_perp_max",
        "fsc_perp_mean",
        "fsc_big_min",
        "fsc_big_max",
        "fsc_big_mean",
        "pe_min",
        "pe_max",
        "pe_mean",
        "chl_small_min",
        "chl_small_max",
        "chl_small_mean",
        "chl_big_min",
        "chl_big_max",
        "chl_big_mean",
        "filter_id",
    ]
    # Construct values string with named placeholders
    values_str = ", ".join([":" + f for f in field_order])
    sql_insert = "INSERT INTO opp VALUES (%s)" % values_str
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
    sql = "SELECT * FROM filter ORDER BY date ASC"
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = pd.read_sql(sql, dbcon)
    return filterdf


def get_latest_filter(dbpath):
    sql = "SELECT * FROM filter ORDER BY date DESC"
    with sqlite3.connect(dbpath) as dbcon:
        filterdf = pd.read_sql(sql, dbcon)
    return filterdf.tail(1)


def get_opp(dbpath, filter_id):
    sql = "SELECT * FROM opp WHERE filter_id = '{}' ORDER BY file".format(filter_id)
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
