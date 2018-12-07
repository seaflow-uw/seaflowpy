import click
from seaflowpy import errors
from seaflowpy import evt
from seaflowpy import fileio

@click.command()
@click.option('-H', '--no-header', is_flag=True, default=False,
    help="Don't print column headers.")
@click.option('-S', '--no-summary', is_flag=True, default=False,
    help="Don't print final summary line.")
@click.option('-v', '--verbose', is_flag=True,
    help='Show information for all files. If not specified then only files errors are printed.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def evt_cmd(no_header, no_summary, verbose, files):
    """Validate EVT files."""
    if not files:
        return
    header_printed = False
    ok, bad = 0, 0
    for evt_file in files:
        if not evt.is_evt(evt_file):
            status = "Filename does not look like an EVT file"
            bad += 1
        else:
            try:
                data = fileio.read_labview(evt_file)
            except errors.FileError as e:
                status = str(e)
                bad += 1
            else:
                status = "OK"
                ok += 1

        if not verbose:
            if status != 'OK':
                if not header_printed and not no_header:
                    print('\t'.join(['path', 'status']))
                    header_printed = True
                print('\t'.join([evt_file, status]))
        else:
            if not header_printed and not no_header:
                print('\t'.join(['path', 'status', 'events']))
                header_printed = True
            if status != "OK":
                print('\t'.join([evt_file, status, '-']))
            else:
                print('\t'.join([evt_file, status, str(len(data.index))]))
    if not no_summary:
        print("%d/%d files passed validation" % (ok, bad + ok))
