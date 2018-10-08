from __future__ import absolute_import, print_function
import click
import os
import sys
from seaflowpy import clouds
from seaflowpy import conf
from seaflowpy import db
from seaflowpy.errors import SeaFlowpyError
from seaflowpy import evt
from seaflowpy import seaflowfile
from seaflowpy import sfl


@click.group()
def sfl_cmd():
    """SFL file operations subcommand."""
    pass


@sfl_cmd.command('convert-gga')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-o', '--outfile', required=True, type=click.File(mode='w', atomic=True),
    help='Output SFL file. - for stdout.')
def sfl_convert_gga_cmd(infile, outfile):
    """Convert GGA coords to decimal degrees."""
    df = sfl.read_files([infile], convert_numerics=False)
    try:
        df = sfl.convert_gga2dd(df)
    except ValueError as e:
        raise click.ClickException(str(e))
    sfl.save_to_file(df, outfile, all_columns=True)


@sfl_cmd.command('db')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-d', '--db', 'dbpath', required=True,
    help='SQLite3 database file.')
@click.option('-c', '--cruise',
    help='Supply a cruise name here to override any found in the filename.')
@click.option('-f', '--force', is_flag=True,
    help='Attempt DB import even if validation produces errors.')
@click.option('-j', '--json', is_flag=True,
    help='Report errors as JSON.')
@click.option('-s', '--serial',
    help='Supply a instrument serial number here to override any found in the filename.')
@click.option('-v', '--verbose', is_flag=True,
    help='Report all errors.')
def sfl_db_cmd(infile, dbpath, cruise, force, json, serial, verbose):
    """Import SFL file data to database.

    Write processed SFL file data to SQLite3 database files. Data will be
    checked before inserting. If any errors are found the first of each type
    will be reported and no data will be written.
    """
    if infile is not sys.stdin:
        # Try to read cruise and serial from filename
        results = sfl.parse_sfl_filename(infile.name)
        if results:
            if cruise is None:
                cruise = results[0]
            if serial is None:
                serial = results[1]

    # Try to read cruise and serial from database if not already defined
    if cruise is None:
        try:
            cruise = db.get_cruise(dbpath)
        except SeaFlowpyError as e:
            pass
    if serial is None:
        try:
            serial = db.get_serial(dbpath)
        except SeaFlowpyError as e:
            pass

    # Make sure cruise and serial are defined somewhere
    if cruise is None or serial is None:
        raise click.ClickException('instrument serial and cruise must both be specified either in filename as <cruise>_<instrument-serial>.sfl, as command-line options, or in database metadata table.')

    df = sfl.read_files([infile])

    df = sfl.fix(df)
    errors = sfl.check(df)

    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=verbose)
        if not force:
            sys.exit(1)
    sfl.save_to_db(df, dbpath, cruise, serial)


@sfl_cmd.command('detect-gga')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
def sfl_detect_gga_cmd(infile):
    """Detect GGA coordinates.

    Detect rows with GGA lat and lon coordinates. If any are found, print
    'True', otherwise print 'False'.
    """
    df = sfl.read_files([infile], convert_numerics=False)
    # Has any GGA coordinates?
    if sfl.has_gga(df):
        click.echo('True')
    else:
        click.echo('False')


@sfl_cmd.command('dedup')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-o', '--outfile', required=True, type=click.File(mode='w', atomic=True),
    help='Output SFL file. - for stdout.')
def sfl_dedup_cmd(infile, outfile):
    """Remove duplicate 'FILE' lines.

    Remove lines with duplicate file entries. Print files removed to stderr.
    """
    df = sfl.read_files([infile], convert_numerics=False)
    dup_files, df = sfl.dedup(df)
    if len(dup_files):
        click.echo(os.linesep.join(['{}\t{}'.format(*d) for d in dup_files]), err=True)
    sfl.save_to_file(df, outfile, all_columns=True)


@sfl_cmd.command('manifest')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-e', '--evt-dir', required=True, metavar='DIR',
    help='All EVT files under this directory will be checked against the SFL listing. If --s3 is set this should be an S3 prefix without a bucket name.')
@click.option('-s', '--s3', is_flag=True,
    help='If set, EVT files are searched for in the configured S3 bucket under the prefix set in --evt_dir.')
@click.option('-v', '--verbose', is_flag=True,
    help='Print a list of all file ids not in common between SFL and directory.')
def manifest_cmd(infile, evt_dir, s3, verbose):
    """Compare files listed in SFL with files found."""
    found_evt_ids = []
    if s3:
        # Make sure configuration for aws is ready to go
        config = conf.get_aws_config()
        cloud = clouds.AWS(config.items('aws'))
        files = cloud.get_files(evt_dir)
        found_evt_files = seaflowfile.sorted_files(evt.parse_file_list(files))
    else:
        found_evt_files = evt.find_evt_files(evt_dir)

    df = sfl.read_files([infile])
    sfl_evt_ids = [seaflowfile.SeaFlowFile(f).file_id for f in df['file']]
    found_evt_ids = [seaflowfile.SeaFlowFile(f).path_file_id for f in found_evt_files]
    sfl_set = set(sfl_evt_ids)
    found_set = set(found_evt_ids)

    print('%d EVT files in SFL file %s' % (len(sfl_set), infile.name))
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
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-o', '--outfile', required=True, type=click.File(mode='w', atomic=True),
    help='Output SFL file. - for stdout.')
def sfl_print_cmd(infile, outfile):
    """Print a standardized version of an SFL file.

    Output columns will match columns selected for database import.
    """
    df = sfl.read_files([infile])
    df = sfl.fix(df)
    sfl.save_to_file(df, outfile, convert_colnames=True)


@sfl_cmd.command('validate')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input SFL file. - for stdin.')
@click.option('-j', '--json', is_flag=True,
    help='Report errors as JSON.')
@click.option('-v', '--verbose', is_flag=True,
    help='Report all errors.')
def sfl_validate_cmd(infile, json, verbose):
    """Validate SFL file.

    Report duplicate files, non-UTC timestamps, missing values in required
    columns, bad coordinates. Only the first error of each type will be
    reported by default.
    """
    df = sfl.read_files([infile])
    df = sfl.fix(df)
    errors = sfl.check(df)
    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=verbose)
