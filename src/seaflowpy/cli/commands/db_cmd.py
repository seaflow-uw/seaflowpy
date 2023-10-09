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
@click.argument('cruise', nargs=1, type=str)
@click.argument('serial', nargs=1, type=str)
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_create_cmd(cruise, serial, db_file):
    """
    Create a popcycle database with cruise and serial number.
    """
    try:
        db.save_df(
            pd.DataFrame({"cruise": [cruise], "inst": [serial]}),
            "metadata",
            db_file,
            clear=True
        )
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
    try:
        errors = db.import_sfl(sfl_file, db_file, force=False)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e
    if len(errors) > 0:
        if json:
            sfl.print_json_errors(errors, sys.stdout, sfl_file, print_all=verbose)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, sfl_file, print_all=verbose)
        if force:
            _ = db.import_sfl(sfl_file, db_file, force=True)


@db_cmd.command('import-filter-params')
@click.option('-p', '--plan',  is_flag=True,
    help="""Create a filter plan for this set of parameters covering all files in SFL table.
            Implies --clear.""")
@click.option('-C', '--clear',  is_flag=True,
    help='Clear existing filter table entries before importing.')
@click.argument('filter-file', nargs=1, type=click.File())
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_import_filter_params_cmd(plan, clear, filter_file, db_file):
    """
    Imports filter parameters to database.

    A new database will be created if it doesn't exist.
    """
    if plan:
        clear = True
    try:
        db.import_filter_params(filter_file, db_file, plan=plan, clear=clear)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e


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
    try:
        db.import_gating_params(
            f"{in_prefix}.gating.tsv",
            f"{in_prefix}.poly.tsv",
            f"{in_prefix}.gating_plan.tsv",
            db_file
        )
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e


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
        db.export_gating_params(db_file, out_prefix)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e


@db_cmd.command('import-outlier')
@click.argument('in-file', nargs=1, type=str)
@click.argument('db-file', nargs=1, type=click.Path(readable=True))
def db_import_outlier_cmd(in_file, db_file):
    """Import outlier table from TSV."""
    try:
        db.import_outlier(in_file, db_file)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e


@db_cmd.command('export-outlier')
@click.option('-p', '--populated', is_flag=True,
    help="Only export if table is populated, i.e. there are flags != 0")
@click.argument('db-file', nargs=1, type=click.Path(readable=True))
@click.argument('out-file', nargs=1, type=str)
def db_export_outlier_cmd(populated, db_file, out_file):
    """Export outlier table as TSV."""
    db.export_outlier(db_file, out_file, populated=populated)


@db_cmd.command('create-filter-plan')
@click.argument('db-file', nargs=1, type=click.Path(writable=True))
def db_create_filter_plan_cmd(db_file):
    """
    Create a filter plan table if sfl and filter tables are populated, overwriting
    any existing plan in the database.
    """
    try:
        filter_plan_df = db.create_filter_plan(db_file)
        db.save_df(filter_plan_df, "filter_plan", db_file, clear=True)
    except SeaFlowpyError as e:
        raise click.ClickException(str(e)) from e
    print("Saved a new filter plan")
    print(filter_plan_df.to_string(index=False))
