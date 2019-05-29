import click
from seaflowpy import errors
from seaflowpy import seaflowfile
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
    """Validate EVT/OPP files."""
    if not files:
        return
    header_printed = False
    ok, bad = 0, 0
    evt_files = seaflowfile.keep_evt_files(files)
    opp_files = seaflowfile.keep_evt_files(files, opp=True)
    evtopp_files = {*(evt_files + opp_files)}
    for filepath in files:
        if filepath not in evtopp_files:
            sff = None
            filetype = 'unknown'
            status = "Filename does not look like an EVT or OPP file"
            bad += 1
        else:
            sff = seaflowfile.SeaFlowFile(filepath)
            try:
                if sff.is_opp:
                    filetype = 'opp'
                    data = fileio.read_opp_labview(filepath)
                else:
                    filetype = 'evt'
                    data = fileio.read_evt_labview(filepath)
            except errors.FileError as e:
                status = str(e)
                bad += 1
            else:
                status = "OK"
                ok += 1

        if not verbose:
            if status != 'OK':
                if not header_printed and not no_header:
                    print('\t'.join(['path', 'type', 'status']))
                    header_printed = True
                print('\t'.join([filepath, filetype, status]))
        else:
            if not header_printed and not no_header:
                print('\t'.join(['path', 'type', 'status', 'events']))
                header_printed = True
            if status != "OK":
                print('\t'.join([filepath, filetype, status, '-']))
            else:
                print('\t'.join([filepath, filetype, status, str(len(data.index))]))
    if not no_summary:
        print("%d/%d files passed validation" % (ok, bad + ok))
