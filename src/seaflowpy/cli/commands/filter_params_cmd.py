"""Import filter parameters to database."""
import click
import pandas as pd
import uuid
from seaflowpy import db
from seaflowpy import seaflowfile
from seaflowpy.errors import SeaFlowpyError



@click.command()
@click.option('-d', '--db', 'dbpath', required=True,
    help='SQLite3 database file.')
@click.option('-i', '--infile', required=True, type=click.File(),
    help='Input filter parameters CSV. - for stdin.')
@click.option('-c', '--cruise',
    help='Supply a cruise name for parameter selection. If not provided cruise in database will be used.')
def filter_params_cmd(dbpath, infile, cruise):
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
        except SeaFlowpyError as e:
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
    else:
        db.save_filter_params(dbpath, params.to_dict('index').values())
