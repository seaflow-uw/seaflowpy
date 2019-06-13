import click
import pkg_resources

version_str = pkg_resources.get_distribution("seaflowpy").version

@click.command()
def version_cmd():
    """Displays version."""
    click.echo(version_str)
