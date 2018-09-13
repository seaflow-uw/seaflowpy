"""Do things to SFL data DataFrames"""
from __future__ import unicode_literals
import arrow
import csv
import json
import os
import pandas as pd
import re
import sys
from . import db
from . import geo
from . import util
from . import seaflowfile
from collections import OrderedDict, Sequence
from past.builtins import basestring


sfl_delim = '\t'

# Mappings between SFL file and SQL table column names
colname_mapping = {
    "table_to_file": {
        "file": "FILE",
        "date": "DATE",
        "file_duration": "FILE DURATION",
        "lat": "LAT",
        "lon": "LON",
        "conductivity": "CONDUCTIVITY",
        "salinity": "SALINITY",
        "ocean_tmp": "OCEAN TEMP",  # not a typo
        "par": "PAR",
        "bulk_red": "BULK RED",
        "stream_pressure": "STREAM PRESSURE",
        "flow_rate": "FLOW RATE",
        "event_rate": "EVENT RATE"
    }
}
# Reverse mappings from SQL table to file column headers
colname_mapping["file_to_table"] = dict([kv[::-1] for kv in colname_mapping["table_to_file"].items()])

# Numeric columns using SQL table column names
numeric_columns = [
    "file_duration", "lat", "lon", "conductivity", "salinity", "ocean_tmp", "par", "bulk_red",
    "stream_pressure", "event_rate"
]

output_columns = [
    "file", "date", "file_duration", "lat", "lon", "conductivity",
    "salinity", "ocean_tmp", "par", "bulk_red", "stream_pressure",
    "event_rate"
]


def check(df):
    """Perform checks on SFL dataframe

    Returns a list of errors.
    """
    errors = []
    errors.extend(check_numerics(df))
    errors.extend(check_file(df))
    errors.extend(check_date(df))
    errors.extend(check_coords(df))
    return errors


def check_coords(df):
    errors = []
    if "lat" not in df.columns:
        errors.append(create_error(df, "lat", msg="lat column is missing"))
    else:
        notnas = df[df["lat"].notna()]
        lat_numbers = pd.to_numeric(notnas["lat"], errors="coerce")
        bad_lats = notnas[~((lat_numbers <= 90) & (lat_numbers >= -90))]["lat"]
        for i, v in bad_lats.iteritems():
            errors.append(create_error(df, "lat", msg="Invalid latitude", row=i, val=v))
    if "lon" not in df.columns:
        errors.append(create_error(df, "lon", msg="lon column is missing"))
    else:
        notnas = df[df["lon"].notna()]
        lon_numbers = pd.to_numeric(notnas["lon"], errors="coerce")
        bad_lons = notnas[~((lon_numbers <= 180) & (lon_numbers >= -180))]["lon"]
        for i, v in bad_lons.iteritems():
            errors.append(create_error(df, "lon", msg="Invalid longitude", row=i, val=v))
    return errors


def check_date(df):
    errors = []
    if "date" not in df.columns:
        errors.append(create_error(df, "date", msg="date column is missing"))
    else:
        # All dates must match RFC 3339 with no fractional seconds
        bad_dates = df[~df["date"].map(lambda d: check_date_string(d))]["date"]
        for i, v in bad_dates.iteritems():
            errors.append(create_error(df, "date", msg="Invalid date format", row=i, val=v))
    return errors


def check_date_string(date):
    """Confirm value is an RFC3339 string with timezone as [+-]00:00"""
    formats = [
        "YYYY-MM-DDTHH:mm:ss\+00:00",
        "YYYY-MM-DDTHH:mm:ss\-00:00"
    ]
    passed = False
    for f in formats:
        try:
            arrow.get(date, f)
        except (arrow.parser.ParserError, ValueError, TypeError):
            pass
        else:
            passed = True
    # Return true if any format is correct
    return passed


def check_file(df):
    errors = []
    # File field must be present
    if "file" not in df.columns:
        errors.append(create_error(df, "file", msg="file column is missing"))
    else:
        # File field must contain well formatted file strings
        old_files_selector = df["file"].str.match(seaflowfile.old_path_re)
        new_files_selector = df["file"].str.match(seaflowfile.new_path_re)
        invalid_files = df[~old_files_selector & ~new_files_selector]["file"]
        for i, v in invalid_files.iteritems():
            errors.append(create_error(df, "file", msg="Invalid file name", row=i, val=v))

        # Files must be unique
        dup_files = df[df.duplicated("file", keep=False)]["file"]
        for i, v in dup_files.iteritems():
            errors.append(create_error(df, "file", msg="Duplicate file", row=i, val=v))

        # Files should be in order
        inorder = seaflowfile.sorted_files(df["file"])
        files_equal = df["file"] == inorder
        if not (files_equal).all():
            i = int(df[~files_equal].index[0])
            v = "First out of order file. Saw {}, expected {}.".format(df.loc[i, "file"], inorder[i])
            errors.append(create_error(df, "file", msg="Files out of order", row=i, val=v))

    return errors


def check_numerics(df):
    errors = []
    # Numeric columns should be present
    present = df.columns.tolist()
    required = set(numeric_columns)
    for column in required.difference(present):
        errors.append(create_error(df, column, msg="{} column is missing".format(column)))
    return errors


def convert_gga2dd(df):
    """Return a copy of df with coordinates converted from GGA to decimal degrees."""
    newdf = df.copy(deep=True)
    newdf["lat"] = df["lat"].map(lambda x: geo.ggalat2dd(x), na_action="ignore")
    newdf["lon"] = df["lon"].map(lambda x: geo.ggalon2dd(x), na_action="ignore")
    return newdf


def create_error(df, col, msg, row=None, val=None):
    """Create an error dictionary"""
    e = {
        "column": col,
        "message": msg,
        "file": None,
        "line (1-based)": None,
        "value": None
    }
    if row is not None:
        e["file"] = df.loc[row, "input_file_path"]
        e["line (1-based)"] = int(df.loc[row, "input_file_line_number"])
    if val is not None:
        e["value"] = make_json_serializable(val)
    elif row is not None:
        e["value"] = make_json_serializable(df.loc[row, col])
    return e


def dedup(df):
    """Remove duplicate rows from DataFrame by "file".

    Returns a 2-tuple of:
        - unique duplicate files, also as 2-tuple of (file_id, count)
        - df without duplicate file rows
    """
    # Find all duplicate files
    dups = df[df.duplicated("file", keep=False)]["file"].tolist()
    # Create a unique list of duplicate file names in order of occurrence
    d = OrderedDict()
    for f in dups:
        if f not in d:
            d[f] = 0
        d[f] += 1
    return (list(d.items()), df.drop_duplicates(subset="file", keep=False))


def find_sfl_files(root):
    """Find all files with .sfl extension beneath root.

    Returns a list of sfl file paths relative to root.
    """
    root = os.path.expanduser(root)
    sfl_paths = []
    for dirpath, dirnames, filenames in os.walk(root):
        for f in filenames:
            if f.endswith(".sfl"):
                sfl_paths.append(os.path.join(dirpath, f))
    return sorted(sfl_paths)


def fix(df):
    """Return a copy of df ready for db import.

    - Adds a "date" column if not present, based on filename datestamp
    - Adds or replaces julian day directory component of "file" values
    - Adds any missing db columns
    """
    newdf = df.copy(deep=True)

    # Add a date column if needed
    if "date" not in newdf.columns:
        newdf["date"] = newdf["file"].map(lambda x: seaflowfile.SeaFlowFile(x).rfc3339)

    # Add julian day directory if needed
    newdf["file"] = newdf["file"].map(lambda x: seaflowfile.SeaFlowFile(x).file_id)

    # Make sure all DB columns are present
    for k in colname_mapping["table_to_file"]:
        if k not in newdf.columns:
            newdf[k] = None

    return newdf


def has_gga(df):
    """Do any coordinates Series in this DataFrame contain GGA values?"""
    gga_lats = df["lat"].map(lambda x: geo.is_gga_lat(x), na_action="ignore")
    gga_lons = df["lon"].map(lambda x: geo.is_gga_lon(x), na_action="ignore")
    return (gga_lats | gga_lons).any()


def make_json_serializable(v):
    """Make sure v is JSON serializable if it's numpy type or plain object"""
    if isinstance(v, pd.np.generic):
        return pd.np.asscalar(v)
    return v


def parse_sfl_filename(fn):
    fn = os.path.basename(fn)
    m = re.match(r"^(?P<cruise>.+)_(?P<inst>[^_]+).sfl$", fn)
    if m:
        return (m.group('cruise'), m.group('inst'))


@util.suppress_sigpipe
def print_json_errors(errors, fh, print_all=True):
    errors_output = []
    errors_seen = set()
    for e in errors:
        if (not print_all) and (e["message"] in errors_seen):
            continue
        errors_seen.add(e["message"])
        errors_output.append(e)
    fh.write(json.dumps(errors_output, sort_keys=True, indent=2, separators=(',',':')))
    fh.write("\n")


@util.suppress_sigpipe
def print_tsv_errors(errors, fh, print_all=True):
    errors_output = []
    errors_seen = set()
    for e in errors:
        if (not print_all) and (e["message"] in errors_seen):
            continue
        errors_seen.add(e["message"])
        errors_output.append(e)
    writer = csv.DictWriter(fh, sorted(errors_output[0].keys()), delimiter='\t', lineterminator=os.linesep)
    writer.writeheader()
    for e in errors_output:
        writer.writerow(e)


def read_files(files, convert_numerics=True, convert_colnames=True, **kwargs):
    """Parse SFL files into one DataFrame.

    Arguments:
    files -- SFL file paths or a single path.

    Keyword arguments:
    convert_numerics -- Cast numeric SQL columns as numbers (default True).
    convert_colnames -- Remap file column names to match SFL SQL table column
        where appropriate. (default True).
    """
    defaults = {
        "sep": str(sfl_delim),
        "dtype": str,
        "na_filter": True,
        "encoding": "utf-8"
    }
    kwargs_defaults = dict(defaults, **kwargs)

    df = None
    if not isinstance(files, Sequence):
        files = [files]
    for f in files:
        partial_df = pd.read_csv(f, **kwargs_defaults)
        # Add column for input file path and file line numbers
        if isinstance(f, basestring):
            partial_df["input_file_path"] = f
        else:
            partial_df["input_file_path"] = f.name
        # Start at 2 to account for zero-based counting and header
        partial_df["input_file_line_number"] = range(2, len(partial_df)+2)
        if df is None:
            df = partial_df
        else:
            df = df.append(partial_df, ignore_index=True)
    df = df.rename(columns=colname_mapping["file_to_table"])

    if convert_numerics:
        for colname in numeric_columns:
            df[colname] = pd.to_numeric(df[colname], errors='coerce')

    if not convert_colnames:
        # Revert column name mapping back to file convention
        df = df.rename(columns=colname_mapping["table_to_file"])

    return df


def save_to_db(df, dbpath, cruise=None, serial=None):
    """Write SFL dataframe to a SQLite3 database.

    Arguments:
    df -- SFL DataFrame.
    dbpath -- Path to SQLite3 database file.
    """
    db.create_db(dbpath)  # create or update db if needed
    if cruise is None:
        cruise = 'None'
    if serial is None:
        serial = 'None'
    metadf = pd.DataFrame({'cruise': [cruise], 'inst': [serial]})
    db.save_metadata(dbpath, metadf.to_dict('index').values())
    # This assumes there are column names which match SQL SFL table
    db.save_sfl(dbpath, df.to_dict('index').values())


@util.suppress_sigpipe
def save_to_file(df, outpath, convert_colnames=True, all_columns=False):
    """Write SFL dataframe to a csv file.

    Arguments:
    df -- SFL DataFrame.
    outpath -- Output file path.

    Keyword Arguments:
    convert_colnames -- Remap SQL table column names to SFL file column names
        where appropriate. (default True).
    """
    # Remove input file path and line number columns that may have been
    # added.
    if "input_file_path" in df.columns and "input_file_line_number" in df.columns:
        df = df.drop(["input_file_path", "input_file_line_number"], axis=1)
    if not all_columns:
        df = df[output_columns]
    if convert_colnames:
        df = df.rename(columns=colname_mapping["table_to_file"])
    df.to_csv(outpath, sep=str(sfl_delim), na_rep="NA", encoding="utf-8",
        index=False, float_format="%.4f")
