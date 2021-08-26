import json
import os
import sys
from glob import glob
import botocore
import click
import tsdataformat
from seaflowpy import clouds
from seaflowpy import db
from seaflowpy import errors as sfperrors
from seaflowpy import fileio
from seaflowpy import seaflowfile
from seaflowpy import sfl



@click.group()
def sfl_cmd():
    """SFL file operations subcommand."""
    pass


@sfl_cmd.command('convert-gga')
@click.argument('sfl-file', nargs=1, type=click.File())
def sfl_convert_gga_cmd(sfl_file):
    """
    Converts GGA coords to decimal degrees.

    To read from STDIN use '-' for SFL_FILE. Prints modified SFL file to STDOUT.
    """
    df = sfl.read_file(sfl_file, convert_numerics=False)
    try:
        df = sfl.convert_gga2dd(df)
    except ValueError as e:
        raise click.ClickException(str(e))
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('detect-gga')
@click.argument('sfl-file', nargs=1, type=click.File())
def sfl_detect_gga_cmd(sfl_file):
    """
    Detects GGA coordinates in SFL_FILE.

    To read from STDIN use '-' for SFL_FILE. Prints 'True' to STDOUT if any GGA
    coordinates are found, else 'False'.
    """
    df = sfl.read_file(sfl_file, convert_numerics=False)
    # Has any GGA coordinates?
    if sfl.has_gga(df):
        click.echo('True')
    else:
        click.echo('False')


@sfl_cmd.command('dedup')
@click.argument('sfl-file', nargs=1, type=click.File())
def sfl_dedup_cmd(sfl_file):
    """
    Removes duplicate 'FILE' lines.

    To read from STDIN use '-' for SFL_FILE. Removes lines with duplicate file
    entries and prints modified SFL to STDOUT. Because it's impossible to know
    which of the duplicated SFL entries corresponds to which EVT file, all
    duplicate rows are removed. Prints a unique list of files removed to STDERR.
    Duplicate files should also be removed from EVT data sets.
    """
    df = sfl.read_file(sfl_file)
    df = sfl.fix(df)
    dup_files, df = sfl.dedup(df)
    if len(dup_files):
        click.echo(os.linesep.join(['{}\t{}'.format(*d) for d in dup_files]), err=True)
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('fix-event-rate')
@click.argument('sfl-file', nargs=1, type=click.Path(exists=True, readable=True))
@click.argument('events-file', nargs=1, type=click.Path(exists=True, readable=True))
def sfl_fix_event_rate_cmd(sfl_file, events_file):
    """
    Calculates true event rates.

    EVENTS-FILE should be a TSV file with EVT path/file ID in first
    column and event count in last column, or a popcycle SQLite3 database file
    with a '.db' extension. A version of SFL_FILE with updated event rates will
    be printed to STDOUT. In cases where the file duration value is < 0 or NA
    the event rate will be NA.
    """
    df = sfl.read_file(sfl_file)
    df = sfl.fix(df)

    # Event counts should be a dict of { file: event_count }
    if events_file.endswith(".db"):
        event_counts = db.get_event_counts(events_file)
    else:
        lines = [x.rstrip().split('\t') for x in events_file.readlines()]
        event_counts = {seaflowfile.SeaFlowFile(x[0]).file_id: int(x[-1]) for x in lines}

    df = sfl.fix_event_rate(df, event_counts)
    sfl.save_to_file(df, sys.stdout)



@sfl_cmd.command('fix-underway')
@click.argument('sfl-file', nargs=1, type=click.Path(exists=True, readable=True))
@click.argument('cruisemic-dir', nargs=1, type=click.Path(exists=True, readable=True))
def sfl_fix_underway_cmd(sfl_file, cruisemic_dir):
    """
    Replace SFL underway data with data from cruisemic.

    A version of SFL_FILE with updated underway columns based on data from the
    cruisemic output directory CRUISEMIC_DIR will be printed to STDOUT.
    """
    df_sfl = sfl.read_file(sfl_file)
    df_sfl = sfl.fix(df_sfl)

    # Read cruisemic metadata file to find underway data
    meta_glob = glob(os.path.join(cruisemic_dir, "metadata*"))
    if len(meta_glob) != 1:
        raise click.ClickException("could not find geo-file")
    with fileio.file_open_r(meta_glob[0], as_text=True) as metafh:
        meta = json.load(metafh)

    geo_filename = meta['GeoFeed']
    thermo_filename = meta['ThermoFeed']
    lat_col = meta['LatitudeCol']
    lon_col = meta['LongitudeCol']
    temp_col = meta['TemperatureCol']
    sal_col = meta['SalinityCol']
    cond_col = meta['ConductivityCol']

    # Read underway data
    geo_glob = glob(os.path.join(cruisemic_dir, geo_filename) + "*")
    thermo_glob = glob(os.path.join(cruisemic_dir, thermo_filename) + "*")
    if len(geo_glob) != 1:
        raise click.ClickException("could not find cruisemic geo data file")
    if len(thermo_glob) != 1:
        raise click.ClickException("could not find cruisemic thermosalinograph file")
    with fileio.file_open_r(geo_glob[0], as_text=True) as geo_fh:
        geo_df = tsdataformat.read_tsdata(geo_fh, convert="time")
        geo_df["lat"] = geo_df[lat_col].astype(float)
        geo_df["lon"] = geo_df[lon_col].astype(float)
        geo_df = geo_df[["time", "lat", "lon"]]
    with fileio.file_open_r(thermo_glob[0], as_text=True) as thermo_fh:
        thermo_df = tsdataformat.read_tsdata(thermo_fh, convert="time")
        thermo_df["ocean_tmp"] = thermo_df[temp_col].astype(float)
        thermo_df["salinity"] = thermo_df[sal_col].astype(float)
        thermo_df["conductivity"] = thermo_df[cond_col].astype(float)
        thermo_df = thermo_df[["time", "ocean_tmp", "salinity", "conductivity"]]

    df = sfl.fix_underway(df_sfl, geo_df, thermo_df)
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('manifest')
@click.option('-v', '--verbose', is_flag=True,
    help='Print a list of all file ids not in common between SFL and directory.')
@click.argument('sfl-file', nargs=1, type=click.File())
@click.argument('evt-dir', nargs=1, type=str)
def manifest_cmd(verbose, sfl_file, evt_dir):
    """
    Compares files in SFL-FILE with files in EVT-DIR.

    If EVT-DIR begins with 's3://s3-bucket-name' then files will located in S3.
    To configure credentials for S3 access use the 'aws' command-line tool from
    the 'awscli' Python package.

    It's normal for about one file per day to be missing from the SFL file or
    EVT day of year folder, especially around midnight.

    To read from STDIN use '-' for SFL_FILE. Prints a file list diff to STDOUT.
    """
    found_evt_ids = []
    if evt_dir.startswith("s3://"):
        try:
            _, _, bucket, evt_dir = evt_dir.split("/", 3)
        except ValueError:
            raise click.ClickException("could not parse bucket and folder from S3 EVT-DIR")
        cloud = clouds.AWS([("s3-bucket", bucket)])
        try:
            files = cloud.get_files(evt_dir)
        except botocore.exceptions.NoCredentialsError:
            print('Please configure aws first:', file=sys.stderr)
            print('  $ pip install awscli', file=sys.stderr)
            print('  then', file=sys.stderr)
            print('  $ aws configure', file=sys.stderr)
            raise click.Abort()
        found_evt_files = seaflowfile.sorted_files(seaflowfile.keep_evt_files(files))
    else:
        found_evt_files = seaflowfile.find_evt_files(evt_dir)

    df = sfl.read_file(sfl_file)
    sfl_evt_ids = [seaflowfile.SeaFlowFile(f).file_id for f in df['file']]
    found_evt_ids = [seaflowfile.SeaFlowFile(f).path_file_id for f in found_evt_files]
    sfl_set = set(sfl_evt_ids)
    found_set = set(found_evt_ids)

    print('%d EVT files in SFL file %s' % (len(sfl_set), sfl_file.name))
    print('%d EVT files in directory %s' % (len(found_set), evt_dir))
    print('%d EVT files in common' % len(sfl_set.intersection(found_set)))
    if verbose and \
       (len(sfl_set.intersection(found_set)) != len(sfl_set) or
        len(sfl_set.intersection(found_set)) != len(found_set)):
        print('')
        print('EVT files in SFL but not found:')
        print(os.linesep.join(sorted(sfl_set.difference(found_set))))
        print('')
        print('EVT files found but not in SFL:')
        print(os.linesep.join(sorted(found_set.difference(sfl_set))))
        print('')


@sfl_cmd.command('print')
@click.argument('sfl-files', metavar='SFL', nargs=-1, type=click.Path(exists=True))
def sfl_print_cmd(sfl_files):
    """
    Concatenates raw SFL files, prints a standardized SFL file.

    Makes the following changes to create a standardized SFL file:

    - Outputs only columns for database import.

    - The correct day of year folder will be added to FILE column values if not
    present

    - DATE column will be created if not present based on "FILE" column values
    (only applies to new-style datestamped file names)

    - STREAM PRESSURE values <= 0 will be changed to 1e-4

    - Any other required columns which are missing will be created with NA
    values.

    Input files will be concatenated in the order they're listed on the
    command-line. Outputs to STDOUT.
    """
    df = None
    for f in sfl_files:
        onedf = sfl.read_file(f)
        onedf = sfl.fix(onedf)
        if df is None:
            df = onedf
        else:
            df = df.append(onedf)
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('validate')
@click.option('-a', '--all', 'report_all', is_flag=True,
    help='Report all errors.')
@click.argument('sfl-file', nargs=-1, type=click.Path(exists=True, readable=True))
def sfl_validate_cmd(report_all, sfl_file):
    """
    Validates SFL files.

    Checks that:

    - Required columns are present: FILE, DATE, FILE DURATION, LAT, LON,
    CONDUCTIVITY, SALINITY, OCEAN TEMP, PAR, BULK RED, STREAM PRESSURE,
    EVENT RATE

    - No missing values in following columns: FILE, DATE, FILE DURATION, LAT,
    LON, STREAM PRESSURE, EVENT RATE

    - FILE column values have day of year folders, are in the proper format, in
    chronological order, are unique, and matches DATE column

    - DATE column values are in the proper format, represent valid dates and
    times, and are UTC

    - FILE DURATION is a positive number

    - LAT and LON column values are valid decimal degree values in the correct
    ranges

    - CONDUCTIVITY, SALINITY, OCEAN TEMP, PAR, BULK_RED column values are
    numbers

    - STREAM PRESSURE is a positive number >= 1e-4

    - EVENT RATE is a positive number

    Because some of these errors can affect every row of the file (e.g. out of
    order files), only the first error of each type is printed. To get a full
    printout of all errors use --all.

    Prints error report to STDOUT.
    """
    need_header = True
    for f in sfl_file:
        try:
            df = sfl.read_file(f)
        except sfperrors.FileError as e:
            raise click.ClickException(str(e))
        errors = sfl.check(df)
        if len(errors) > 0:
            sfl.print_tsv_errors(errors, sys.stdout, os.path.basename(f),
                                 print_all=report_all, header=need_header)
        need_header = False
