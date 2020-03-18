import click
from seaflowpy import seaflowfile
from seaflowpy import errors


@click.command()
@click.option('-v', '--verbose', is_flag=True,
    help='Print 3 columns: input path, file name, day of year dir.')
@click.argument('files', nargs=-1, type=click.Path())
def dayofyear_cmd(verbose, files):
    """
    Gets calculated day of year dir from filename timestamp.

    File paths must be new-style datestamped paths. Any part of the file
    path except for the filename will be ignored. The filename may include a
    '.gz' extension. Outputs to STDOUT.
    """
    output = []
    for file in files:
        try:
            sfile = seaflowfile.SeaFlowFile(file)
        except errors.FileError as e:
            click.echo("%s %s" % (file, e), err=True)
            continue
        if verbose:
            output.append([file, sfile.file_id, sfile.filename, sfile.dayofyear])
        else:
            output.append([sfile.dayofyear])
    if output:
        click.echo("\n".join(["\t".join(row) for row in output]))
