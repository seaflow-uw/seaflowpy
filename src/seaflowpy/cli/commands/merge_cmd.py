"""Import SFl file into sqlite3 database."""
import click
from seaflowpy import db
from seaflowpy import errors


@click.command()
@click.argument('db1', type=click.Path(exists=True))
@click.argument('db2', type=click.Path(exists=True))
def merge_cmd(db1, db2):
    """Merge SQLite3 db1 into db2.

    Only merges gating, poly, filter tables.
    """
    db.merge_dbs(db1, db2)
