import sys
import click
import pandas as pd
from pathlib import Path
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
@click.option('-c', '--cruise', type=str,
    help='Supply a cruise name to be saved in the database.',)
@click.option('-s', '--serial', type=str,
    help='Supply an instrument serial number to be saved in the database.')
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_create_cmd(cruise, serial, db_file):
    """
    Create or update a popcycle database with cruise and serial number.
    """
    # Try to read cruise and serial from database

    if not cruise:
        click.echo('no cruise provided, pulling from database')
        try:
            cruise = db.get_cruise(db_file)
        except SeaFlowpyError as e:
            raise click.ClickException(e)
    if not serial:
        click.echo('no serial provided, pulling from database')
        try:
            serial = db.get_serial(db_file)
        except SeaFlowpyError as e:
            raise click.ClickException(e)

    try:
        # This step also creates a new database file if necessary or updates an
        # existing database's schema.
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
    if Path(db_file).exists():
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
@click.option('-p', '--plan',  is_flag=True,
    help='Create a filter plan for this set of parameters covering all files in SFL table')
@click.argument('filter-file', nargs=1, type=click.File())
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_import_filter_params_cmd(cruise, plan, filter_file, db_file):
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
    if plan:
        try:
            db.create_filter_plan(db_file)
        except SeaFlowpyError as e:
            raise click.ClickException(str(e))


@db_cmd.command('import-gating-params')
@click.argument('in-prefix', nargs=1, type=str)
@click.argument('db-file', nargs=1, type=click.Path(readable=True))
def db_import_gating_params_cmd(in_prefix, db_file):
    """
    Import gating parameters.

    Inverse of export-gating-params. in-prefix should match three files
    corresponding to three gating parameter db tables that will be
    populated/overwritten: gating (in-prefix.gating.tsv),
    poly (in-prefix.poly.tsv), and gating_plan (in-prefix.gating_plan.tsv).
    """
    gating_df = pd.read_csv(f"{in_prefix}.gating.tsv", sep="\t", dtype="string")
    poly_df = pd.read_csv(f"{in_prefix}.poly.tsv", sep="\t", dtype="string")
    gating_plan_df = pd.read_csv(f"{in_prefix}.gating_plan.tsv", sep="\t", dtype="string")

    db.save_df(db_file, "gating", gating_df, delete_first=True)
    db.save_df(db_file, "poly", poly_df, delete_first=True)
    db.save_df(db_file, "gating_plan", gating_plan_df, delete_first=True)


@db_cmd.command('export-gating-params')
@click.argument('db-file', nargs=1, type=click.Path(readable=True))
@click.argument('out-prefix', nargs=1, type=str)
def db_export_gating_params_cmd(db_file, out_prefix):
    """
    Export gating parameters.

    Three output TSV files will be created, representing the three gating
    parameter db tables: gating (out-prefix.gating.tsv),
    poly (out-prefix.poly.tsv), and gating_plan (out-prefix.gating_plan.tsv).

    If the gating_plan table isn't present or populated in db_file a gating_plan
    will attempt to be created from the vct table.
    """
    try:
        gating_df = db.get_gating_table(db_file)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e))

    try:
        poly_df = db.get_poly_table(db_file)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e))

    try:
        gating_plan_df = db.get_gating_plan_table(db_file)
    except SeaFlowpyError as e:
        # Maybe this is an older db schema without gating_plan table
        gating_plan_df = None

    if gating_plan_df is None or len(gating_plan_df) == 0:
        click.echo("no gating_plan in db, inferring from vct table")
        gating_plan_df = db.create_gating_plan_from_vct(db_file)
        if len(gating_plan_df) == 0:
            raise click.ClickException("could not create gating_plan from db")

    Path(out_prefix).parent.mkdir(exist_ok=True, parents=True)
    gating_df.to_csv(f"{out_prefix}.gating.tsv", sep="\t", index=False)
    poly_df.to_csv(f"{out_prefix}.poly.tsv", sep="\t", index=False)
    gating_plan_df.to_csv(f"{out_prefix}.gating_plan.tsv", sep="\t", index=False)


@db_cmd.command('create-filter-plan')
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_create_filter_plan_cmd(db_file):
    """
    Create a filter plan table if sfl and filter tables are populated.
    """
    try:
        filter_plan_df = db.create_filter_plan(db_file)
    except SeaFlowpyError as e:
        raise click.ClickException(e)
    print("Saved a new filter plan")
    print(filter_plan_df.to_string(index=False))


@db_cmd.command('merge')
@click.argument('db1', type=click.Path(exists=True))
@click.argument('db2', type=click.Path(exists=True, writable=True))
def db_merge_cmd(db1, db2):
    """Merges SQLite3 DB1 into DB2.

    Only merges gating, poly, filter tables.
    """
    db.merge_dbs(db1, db2)
