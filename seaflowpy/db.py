import sqlite3
import uuid
import util

def ensure_tables(dbpath):
    """Ensure all popcycle tables exists."""
    con = sqlite3.connect(dbpath)
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS vct (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
        pop TEXT NOT NULL,
        count INTEGER NOT NULL,
        method TEXT NOT NULL,
        fsc_small REAL NOT NULL,
        fsc_perp REAL NOT NULL,
        pe REAL NOT NULL,
        chl_small REAL NOT NULL,
        gating_id TEXT NOT NULL,
        PRIMARY KEY (cruise, file, pop)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS opp (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
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
        PRIMARY KEY (cruise, file)
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

    cur.execute("""CREATE TABLE IF NOT EXISTS cytdiv (
        cruise TEXT NOT NULL,
        file TEXT NOT NULL,
        N0 INTEGER,
        N1 REAL,
        H REAL,
        J REAL,
        opp_red REAL,
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
        pop_order TEXT NOT NULL,
        PRIMARY KEY (id)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS poly (
        pop TEXT NOT NULL,
        fsc_small REAL,
        fsc_perp REAL,
        fsc_big REAL,
        pe REAL,
        chl_small REAL,
        chl_big REAL,
        gating_id TEXT NOT NULL
    )""")

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

def execute(dbpath, sql, values=None, timeout=120):
    con = sqlite3.connect(dbpath, timeout=timeout)
    if values is not None:
        con.execute(sql, values)
    else:
        con.execute(sql)
    con.commit()
    con.close()
