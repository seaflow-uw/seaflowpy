import click
from seaflowpy.cli.commands.dayofyear_cmd import dayofyear_cmd
from seaflowpy.cli.commands.db_cmd import db_cmd
from seaflowpy.cli.commands.evt_cmd import evt_cmd
from seaflowpy.cli.commands.filter_cmd import filter_cmd
from seaflowpy.cli.commands.opp_cmd import opp_cmd
from seaflowpy.cli.commands.sfl_cmd import sfl_cmd
from seaflowpy.cli.commands.sds2sfl_cmd import sds2sfl_cmd
from seaflowpy.cli.commands.version_cmd import version_cmd

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
def run():
    pass

run.add_command(dayofyear_cmd, 'dayofyear')
run.add_command(db_cmd, 'db')
run.add_command(evt_cmd, 'evt')
run.add_command(filter_cmd, 'filter')
run.add_command(opp_cmd, 'opp')
run.add_command(sds2sfl_cmd, 'sds2sfl')
run.add_command(sfl_cmd, 'sfl')
run.add_command(version_cmd, 'version')
