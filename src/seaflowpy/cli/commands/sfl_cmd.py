import os
import sys
import click
from seaflowpy import clouds
from seaflowpy import conf
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
    df = sfl.read_file(sfl_file)
    try:
        df = sfl.convert_gga2dd(df)
    except ValueError as e:
        raise click.ClickException(str(e))
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('detect-gga')
@click.argument('sfl-file', nargs=1, type=click.File())
def sfl_detect_gga_cmd(infile):
    """
    Detects GGA coordinates in SFL_FILE.

    To read from STDIN use '-' for SFL_FILE. Prints 'True' to STDOUT if any GGA
    coordinates are found, else 'False'.
    """
    df = sfl.read_file(infile)
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
@click.argument('sfl-file', nargs=1, type=click.File())
@click.argument('events-file', nargs=1, type=click.File())
def sfl_fix_event_rate_cmd(sfl_file, events_file):
    """
    Calculates true event rates.

    To read SFL_FILE or EVENTS_FILE from STDIN use '-', but obviously not for
    both. EVENTS-FILE should be a TSV file with EVT path/file ID in first
    column and event count in last column. A version of SFL_FILE with updated
    event rates will be printed to STDOUT. In cases where the file duration
    value is < 0 or NA the event rate will be NA.
    """
    df = sfl.read_file(sfl_file)
    df = sfl.fix(df)
    lines = [x.rstrip().split('\t') for x in events_file.readlines()]
    event_counts = {seaflowfile.SeaFlowFile(x[0]).file_id: int(x[-1]) for x in lines}
    df = sfl.fix_event_rate(df, event_counts)
    sfl.save_to_file(df, sys.stdout)


@sfl_cmd.command('manifest')
@click.option('-s', '--s3', is_flag=True,
    help='If set, EVT files are searched for in the configured S3 bucket under the prefix set in --evt_dir.')
@click.option('-v', '--verbose', is_flag=True,
    help='Print a list of all file ids not in common between SFL and directory.')
@click.argument('sfl-file', nargs=1, type=click.File())
@click.argument('evt-dir', nargs=1, type=str)
def manifest_cmd(s3, verbose, sfl_file, evt_dir):
    """
    Compares files in SFL-FILE with files in EVT-DIR.

    If --s3 is set then EVT-DIR should be an S3 prefix without a bucket name.
    The bucket name will be filled in based on seaflowpy config. Iit's normal
    for one file to be missing from the SFL file or EVT day of year folder
    around midnight. To read from STDIN use '-' for SFL_FILE. Prints a file list
    diff to STDOUT.
    """
    found_evt_ids = []
    if s3:
        # Make sure configuration for aws is ready to go
        config = conf.get_aws_config()
        cloud = clouds.AWS(config.items('aws'))
        files = cloud.get_files(evt_dir)
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

    Outputs only columns for database import. The correct day of year folder
    will be added to "FILE" column values if not present. "DATE" column will be
    created if not present based on "FILE" column values (only applies to
    new-style datestamped file names). Any other required columns which are
    missing will be created with "NA" values. Input files will be concatenated
    in the order they're listed on the command-line. Outputs to STDOUT.
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
@click.option('-j', '--json', is_flag=True,
    help='Report errors as JSON.')
@click.option('-v', '--verbose', is_flag=True,
    help='Report all errors.')
@click.argument('sfl-file', nargs=1, type=click.File())
def sfl_validate_cmd(json, verbose, sfl_file):
    """
    Validates an SFL file.

    Checks that:
    all required columns are present;
    there are no missing values in columns that don't allow NULLs (FILE, DATE, LAT, LON);
    "FILE" column values have day of year folders, are in the proper format, in chronological order, and are unique;
    "DATE" column values are in the proper format, represent valid date and times, and are UTC;
    "LAT" and "LON" coordinate column values are valid decimal degree values.

    Because some of these errors can affect every row of the file (e.g. out of
    order files), only the first error of each type is printed. To get a full
    printout of all errors use --verbose.

    To read from STDIN use '-' for SFL_FILE. Prints to STDOUT.
    """
    df = sfl.read_file(sfl_file)
    errors = sfl.check(df)
    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=verbose)
