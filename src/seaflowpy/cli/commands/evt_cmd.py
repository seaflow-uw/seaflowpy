import os
import click
from seaflowpy import errors
from seaflowpy import seaflowfile
from seaflowpy import fileio


@click.group()
def evt_cmd():
    """EVT file examination subcommand."""
    pass


@evt_cmd.command('validate')
@click.option('-H', '--no-header', is_flag=True, default=False,
    help="Don't print column headers.")
@click.option('-S', '--no-summary', is_flag=True, default=False,
    help="Don't print final summary line.")
@click.option('-v', '--verbose', is_flag=True,
    help='Show information for all files. If not specified then only files errors are printed.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def validate_evt_cmd(no_header, no_summary, verbose, files):
    """
    Examines EVT/OPP files.

    If any of the file arguments are directories all EVT/OPP files within those
    directories will be recursively found and examined. Prints to STDOUT.
    """
    if not files:
        return

    files = expand_file_list(files)  # dirs to file paths
    evt_files = seaflowfile.keep_evt_files(files)
    opp_files = seaflowfile.keep_evt_files(files, opp=True)
    evtopp_files = {*(evt_files + opp_files)}

    header_printed = False
    ok, bad = 0, 0

    for filepath in files:
        # Default values
        filetype = '-'
        file_id = '-'
        events = 0

        if filepath not in evtopp_files:
            status = "Filename does not look like an EVT or OPP file"
            bad += 1
        else:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id

            try:
                if sff.is_opp:
                    filetype = 'opp'
                    data = fileio.read_opp_labview(filepath)
                    events = len(data.index)
                else:
                    filetype = 'evt'
                    data = fileio.read_evt_labview(filepath)
                    events = len(data.index)
            except errors.FileError as e:
                status = str(e)
                bad += 1
            else:
                status = "OK"
                ok += 1

        if not verbose:
            if status != 'OK':
                if not header_printed and not no_header:
                    print('\t'.join(['path', 'file_id', 'type', 'status', 'events']))
                    header_printed = True
                print('\t'.join([filepath, file_id, filetype, status, str(events)]))
        else:
            if not header_printed and not no_header:
                print('\t'.join(['path', 'file_id', 'type', 'status', 'events']))
                header_printed = True
            print('\t'.join([filepath, file_id, filetype, status, str(events)]))
    if not no_summary:
        print("%d/%d files passed validation" % (ok, bad + ok))


@evt_cmd.command('count')
@click.option('-H', '--no-header', is_flag=True, default=False,
    help="Don't print column headers.")
@click.argument('evt-files', nargs=-1, type=click.Path(exists=True))
def count_evt_cmd(no_header, evt_files):
    """
    Reports event counts in EVT files.

    For speed, only a portion at the beginning of the file is read to get the
    event count. If any of EVT-FILES are directories all EVT/OPP files within
    those directories will be recursively found and examined. Files which can't
    be read with a valid EVT/OPP file name and file header will be reported
    with a count of 0.

    Unlike the "evt validate" command, this command does not attempt validation
    of the EVT/OPP file beyond reading the first 4 byte row count header.
    Because of this, there may be files where "evt validate" reports 0 rows
    while this tool reports > 0 rows.

    Outputs tab-delimited text to STDOUT.
    """
    if not evt_files:
        return

    files = expand_file_list(evt_files)  # dirs to file paths
    evt_files = seaflowfile.keep_evt_files(files)
    opp_files = seaflowfile.keep_evt_files(files, opp=True)
    evtopp_files = {*(evt_files + opp_files)}

    header_printed = False

    for filepath in files:
        # Default values
        filetype = '-'
        file_id = '-'
        events = 0

        if filepath in evtopp_files:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
            try:
                if sff.is_opp:
                    filetype = 'opp'
                else:
                    filetype = 'evt'
                events = fileio.read_labview_row_count(filepath)
            except errors.FileError:
                pass  # accept defaults, do nothing

        if not header_printed and not no_header:
            print('\t'.join(['path', 'file_id', 'type', 'events']))
            header_printed = True
        print('\t'.join([filepath, file_id, filetype, str(events)]))


def expand_file_list(files):
    """Convert directories in file list to EVT/OPP file paths."""
    # Find files in directories
    dirs = [f for f in files if os.path.isdir(f)]
    files = [f for f in files if os.path.isfile(f)]
    for d in dirs:
        evt_files = seaflowfile.find_evt_files(d)
        opp_files = seaflowfile.find_evt_files(d, opp=True)
        files = files + evt_files + opp_files
    return files
