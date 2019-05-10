import click
import pkg_resources

version_str = pkg_resources.get_distribution("seaflowpy").version

@click.command()
def version_cmd():
    """Display version."""
    click.echo(version_str)
