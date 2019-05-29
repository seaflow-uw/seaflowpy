import sys
import click
import pandas as pd
from seaflowpy import db
from seaflowpy.errors import SeaFlowpyError
from seaflowpy import sfl


@click.group()
def db_cmd():
    """Database file operations subcommand."""
    pass


@db_cmd.command('create')
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
def db_create_cmd(infile, dbpath, cruise, force, json, serial, verbose):
    """Create database from SFL file.

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

    df = sfl.read_file(infile)

    df = sfl.fix(df)
    errors = sfl.check(df)

    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=verbose)
        if not force and len([e for e in errors if e["level"] == "error"]) > 0:
            sys.exit(1)
    sfl.save_to_db(df, dbpath, cruise, serial)


@db_cmd.command('import-filter-params')
@click.option('-d', '--db', 'dbpath', required=True,
    help='SQLite3 database file.')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input filter parameters CSV. - for stdin.')
@click.option('-c', '--cruise',
    help='Supply a cruise name for parameter selection. If not provided cruise in database will be used.')
def db_import_filter_params_cmd(dbpath, infile, cruise):
    """Import filter parameters to database.

    File paths must be new-style datestamped paths. Any part of the file
    path except for the filename will be ignored. The filename may include a
    '.gz' extension. If an entry can't be found for the specified cruise this
    command will exit with a non-zero exit status.
    """
    # If cruise not supplied, try to get from db
    if cruise is None:
        try:
            cruise = db.get_cruise(dbpath)
        except SeaFlowpyError:
            pass

    if cruise is None:
        raise click.ClickException('cruise must be specified either as command-line option or in database metadata table.')

    defaults = {
        "sep": str(','),
        "na_filter": True,
        "encoding": "utf-8"
    }
    df = pd.read_csv(infile, **defaults)
    df.columns = [c.replace('.', '_') for c in df.columns]
    params = df[df.cruise == cruise]
    if len(params.index) == 0:
        raise click.ClickException('no filter parameters found for cruise %s' % cruise)
    db.save_filter_params(dbpath, params.to_dict('index').values())


@db_cmd.command('merge')
@click.argument('db1', type=click.Path(exists=True))
@click.argument('db2', type=click.Path(exists=True))
def db_merge_cmd(db1, db2):
    """Merge SQLite3 db1 into db2.

    Only merges gating, poly, filter tables.
    """
    db.merge_dbs(db1, db2)
