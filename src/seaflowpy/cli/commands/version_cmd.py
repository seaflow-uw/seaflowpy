import click
import seaflowpy as sfp


@click.command()
def version_cmd():
    """Displays version."""
    click.echo(sfp.__version__)
