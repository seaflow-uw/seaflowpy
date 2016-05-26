"""Import SFl file into sqlite3 database."""
import argparse
import datetime
import dateutil.parser
import db
import geo
import glob
import os
import pkg_resources
import re
import sys


DELIM = '\t'

CRUISE = 'CRUISE'                          # str
FILE = 'FILE'                              # str
DATE = 'DATE'                              # str
FILE_DURATION = 'FILE DURATION'            # float
LAT = 'LAT'                                # string --> Format: Decimal Degrees (DDD) or GGA
LON = 'LON'                                # string --> Format: Decimal Degrees (DDD) or GGA
CONDUCTIVITY = 'CONDUCTIVITY'              # float
SALINITY = 'SALINITY'                      # float
OCEAN_TEMP = 'OCEAN TEMP'                  # float
PAR = 'PAR'                                # float
BULK_RED  = 'BULK RED'                     # float
STREAM_PRESSURE  = 'STREAM PRESSURE'       # float
FLOW_RATE = 'FLOW RATE'                    # float
EVENT_RATE  = 'EVENT RATE'                 # float

FLOATS = [FILE_DURATION, SALINITY, OCEAN_TEMP, BULK_RED, STREAM_PRESSURE, FLOW_RATE, CONDUCTIVITY, PAR, EVENT_RATE]
STRS = [FILE, DATE, LAT, LON]

FILE_COLUMNS = [
    'CRUISE', 'FILE', 'DATE', 'FILE_DURATION', 'LAT', 'LON', 'CONDUCTIVITY',
    'SALINITY', 'OCEAN_TEMP', 'PAR', 'BULK_RED', 'STREAM_PRESSURE',
    'FLOW_RATE','EVENT_RATE'
]


def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    parser = argparse.ArgumentParser(
        description='A program to insert SFL file data into a popcycle sqlite3 database (version %s)' % version)

    parser.add_argument(
        '-c', '--cruise',
        required=True,
        help='cruise name, e.g. CMOP_3')
    parser.add_argument(
        '-g', "--gga", action='store_true',
        help='lat/lon input is in GGA format. Convert to decimal degree.')
    parser.add_argument(
        '-w', '--west', action='store_true',
        help="""Some ships don't provide E/W designations for longitude. Use
        this flag if this is the case and all longitudes should be West
        (negative).""")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-e', '--evt_dir',
        help='EVT data directory if specific SFL file not provided, e.g ~/SeaFlow/datafiles/evt/')
    group.add_argument(
        '-s', '--sfl',
        help='SFL file if EVT directory is not provided.')

    parser.add_argument(
        '-d', '--db',
        required=True,
        help='''sqlite3 database file, e.g. ~/popcycle/sqlite/popcycle.db. Will
             be created with just an sfl table if doesn't exist.''')

    parser.add_argument("--version", action="version", version="%(prog)s " + version)

    return parser.parse_args()


def fix_one_sfl_line(data, header, cruise, gga=False, west=False):
    """Convert one line of SFL file into dict ready for db insert"""
    dbcolumn_to_fixed_data = {}

    for d, h in zip(data, header):
        h = h.upper()
        h = h.strip('\n')
        d = d.strip('\n')
        if h in FLOATS:
            h = h.strip().replace(' ', '_')
            try:
                dbcolumn_to_fixed_data[h] = float(d)
            except ValueError:
                dbcolumn_to_fixed_data[h] = None
        elif h in STRS:
            h = h.strip().replace(' ', '_')
            dbcolumn_to_fixed_data[h] = d
        # else, do nothing

    # add cruise, date, and add julian day if missing
    dbcolumn_to_fixed_data[CRUISE] = cruise
    if "DATE" in dbcolumn_to_fixed_data:
        # Input is an SFL converted from SDS which has a DATE column
        pass
    else:
        # Input is new style SFL where date is parsed from file
        dbcolumn_to_fixed_data[DATE] = date_from_file_name(dbcolumn_to_fixed_data[FILE])

    if len(dbcolumn_to_fixed_data[FILE].split("/")) == 1:
        # Add julian day directory if missing
        dbcolumn_to_fixed_data[FILE] = "/".join([
            julian_from_file_name(dbcolumn_to_fixed_data[FILE]),
            dbcolumn_to_fixed_data[FILE]
        ])

    # any fields that weren't passed in should be present and None
    for c in FILE_COLUMNS:
        if not c in dbcolumn_to_fixed_data:
            dbcolumn_to_fixed_data[c] = None

    # populate values dict
    values = {}
    for c in FILE_COLUMNS :
        values[c.lower()] = dbcolumn_to_fixed_data[c]
    # File column OCEAN_TEMP was misnamed to ocean_tmp in the sqlite3 sfl
    # table. Rather than fix this everywhere I'm going to just make sure
    # there is an ocean_tmp entry here.
    values["ocean_tmp"] = dbcolumn_to_fixed_data['OCEAN_TEMP']

    if values["lon"] is not None:
        if gga:
            values["lon"] = geo.gga2dd(values["lon"])
        if west:
            values["lon"] = geo.westify_dd_lon(values["lon"])
    if values["lat"] is not None and gga:
        values["lat"] = geo.gga2dd(values["lat"])

    return values


def insert_files(sfl_files, dbpath, cruise, gga=False, west=False):
    for sfl_file in sfl_files:
        lines = open(sfl_file).readlines()
        header = lines[0].split('\t')
        to_insert = []
        for line in lines[1:]:
            data = line.split('\t')
            record = fix_one_sfl_line(data, header, cruise, gga=gga, west=west)
            to_insert.append(record)
        db.save_sfl(dbpath, to_insert)


def insert_files_recursive(dbpath, evt_path, cruise, gga=False, west=False):
    dbpath = os.path.expanduser(dbpath)
    evt_path = os.path.expanduser(evt_path)
    if not os.path.isdir(evt_path):
        raise ValueError("%s is not directory or does not exist" % evt_path)
    insert_files(find_sfl_files(evt_path), dbpath, cruise, gga=gga, west=west)


def find_sfl_files(evt_path):
    evt_path = os.path.expanduser(evt_path)
    sfl_paths = []
    for dirpath, dirnames, filenames in os.walk(evt_path):
        for f in filenames:
            if f.endswith(".sfl"):
                sfl_paths.append(os.path.join(dirpath, f))
    return sfl_paths


def date_from_file_name(file_name):
    date = None
    match = re.match(r'(\d{4}-\d{2}-\d{2})T(\d{2}-\d{2}-\d{2})([+-]\d{2}-?\d{2})',
                     file_name)
    if match:
        # New style EVT file names, e.g.
        # - 2014-05-15T17-07-08+0000
        # - 2014-05-15T17-07-08+00-00
        # - 2014-05-15T17-07-08-0700
        # - 2014-05-15T17-07-08-07-00

        # Convert to a ISO 8601 date string
        datestamp, timestamp, tz = match.groups()
        if len(tz) > 5:
            # Remove middle "-" in timezone substring
            # e.g. +00-00 or +00:00 becomes +0000
            tz = tz[:3] + tz[4:]
        # SeaFlow EVT file names have "-"s instead of ":"s due to filesystem
        # naming rules. Fix things up to make valid time strings here
        timestamp = timestamp.replace('-', ':')
        # Put it all back together
        date = datestamp + 'T' + timestamp + tz

    if date is None:
        raise ValueError("Could not parse file name %s\n" % file_name)

    return date


def julian_from_file_name(file_name):
    """Converts a dated EVT file name to a Seaflow day of year folder name.

    "2014-07-04T00-00-02+00-00" or "2014-07-04T00-00-02+0000" would return
    "2014_185".

    Args:
        evt_filename: EVT filename, may include path information
    """
    iso8601 = date_from_file_name(os.path.basename(file_name))
    dt = dateutil.parser.parse(iso8601)
    dt_jan1 = datetime.date(dt.year, 1, 1)
    day = dt.toordinal() - dt_jan1.toordinal() + 1
    return "%i_%i" % (dt.year, day)


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    db.ensure_tables(args.db)
    db.ensure_indexes(args.db)

    if args.evt_dir:
        # Try to insert all SFl files in EVT dir
        insert_files_recursive(
            args.db, args.evt_dir, args.cruise, gga=args.gga, west=args.west)
    else:
        # User specified SFL file
        insert_files(
            [args.sfl], args.db, args.cruise, gga=args.gga, west=args.west)


if __name__ == "__main__":
    main()
