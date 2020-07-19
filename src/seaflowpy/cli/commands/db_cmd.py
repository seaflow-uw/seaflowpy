import sys
import click
from seaflowpy import db
from seaflowpy.errors import SeaFlowpyError
from seaflowpy import fileio
from seaflowpy import sfl

# Subcommand aliases for backwards compatibility
aliases = {'create': 'import-sfl'}


class AliasedGroup(click.Group):

    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        try:
            real_cmd = aliases[cmd_name]
            return click.Group.get_command(self, ctx, real_cmd)
        except KeyError:
            return None


@click.command(cls=AliasedGroup)
def db_cmd():
    """Database file operations subcommand."""
    pass


@db_cmd.command('create')
@click.option('-d', '--db', 'db_file', required=True, type=click.Path(writable=True),
    help='DB file to create or update.')
@click.option('-c', '--cruise', required=True,
    help='Supply a cruise name to be saved in the database.',)
@click.option('-s', '--serial', required=True,
    help='Supply an instrument serial number to be saved in the database.')
def db_create_cmd(db_file, cruise, serial):
    """
    Create or updated a popcycle database with cruise and serial number.
    """
    try:
        db.save_metadata(db_file, [{"cruise": cruise, "inst": serial}])
    except SeaFlowpyError as e:
        raise click.ClickException(str(e))


@db_cmd.command('import-sfl')
@click.option('-f', '--force', is_flag=True,
    help='Attempt DB import even if validation produces errors.')
@click.option('-j', '--json', is_flag=True,
    help='Report errors as JSON.')
@click.option('-v', '--verbose', is_flag=True,
    help='Report all errors.')
@click.argument('sfl-file', nargs=1, type=click.Path(exists=True))
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_import_sfl_cmd(force, json, verbose, sfl_file, db_file):
    """
    Imports SFL metadata to database.

    Writes processed SFL-FILE data to SQLite3 database file. Data will be
    checked before inserting. If any errors are found the first of each type
    will be reported and no data will be written. SFL-FILE may have the 
    <cruise name> and <instrument serial>
    embedded in the filename as '<cruise name>_<instrument serial>.sfl'. If not,
    it's expected that information is already in the database. Databae cruise and
    serial overrides values in filename. If a database
    file does not exist a new one will be created. Any SFL data in the database
    will be erased before importing new data. Errors or warnings are output to
    STDOUT.
    """
    cruise, serial = None, None

    # Try to read cruise and serial from database
    try:
        cruise = db.get_cruise(db_file)
    except SeaFlowpyError as e:
        pass
    try:
        serial = db.get_serial(db_file)
    except SeaFlowpyError as e:
        pass

    # Try to read cruise and serial from filename if not already defined
    if cruise is None or serial is None:
        results = sfl.parse_sfl_filename(sfl_file)
        if results:
            cruise = results[0]
            serial = results[1]

    # Make sure cruise and serial are defined somewhere
    if cruise is None or serial is None:
        raise click.ClickException('instrument serial and cruise must both be specified either in filename as <cruise>_<instrument-serial>.sfl, as command-line options, or in database metadata table.')

    df = sfl.read_file(sfl_file)

    df = sfl.fix(df)
    errors = sfl.check(df)

    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, sfl_file, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, sfl_file, print_all=verbose)
        if not force and len([e for e in errors if e["level"] == "error"]) > 0:
            sys.exit(1)
    sfl.save_to_db(df, db_file, cruise, serial)


@db_cmd.command('import-filter-params')
@click.option('-c', '--cruise',
    help='Supply a cruise name for parameter selection. If not provided cruise in database will be used.')
@click.argument('filter-file', nargs=1, type=click.File())
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_import_filter_params_cmd(cruise, filter_file, db_file):
    """
    Imports filter parameters to database.

    A new database will be created if it doesn't exist.
    """
    # If cruise not supplied, try to get from db
    if cruise is None:
        try:
            cruise = db.get_cruise(db_file)
            print("using cruise={} from db".format(cruise))
        except SeaFlowpyError:
            pass

    if cruise is None:
        raise click.ClickException('cruise must be specified either as command-line option or in database metadata table.')

    df = fileio.read_filter_params_csv(filter_file)
    params = df[df.cruise == cruise]
    if len(params.index) == 0:
        raise click.ClickException('no filter parameters found for cruise %s' % cruise)
    db.save_filter_params(db_file, params.to_dict('index').values())


@db_cmd.command('merge')
@click.argument('db1', type=click.Path(exists=True))
@click.argument('db2', type=click.Path(exists=True, writable=True))
def db_merge_cmd(db1, db2):
    """Merges SQLite3 DB1 into DB2.

    Only merges gating, poly, filter tables.
    """
    db.merge_dbs(db1, db2)
