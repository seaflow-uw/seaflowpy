import os
import click
from seaflowpy import errors
from seaflowpy import seaflowfile
from seaflowpy import fileio
from seaflowpy import sample


@click.group()
def evt_cmd():
    """EVT file examination subcommand."""
    pass


@evt_cmd.command('count')
@click.option('-H', '--no-header', is_flag=True, default=False, show_default=True,
    help="Don't print column headers.")
@click.argument('evt-files', nargs=-1, type=click.Path(exists=True))
def count_evt_cmd(no_header, evt_files):
    """
    Reports event counts in EVT/OPP files.

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

    # dirs to file paths
    files = expand_file_list(evt_files)

    header_printed = False

    for filepath in files:
        # Default values
        filetype = '-'
        file_id = '-'
        events = 0
        try:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
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


def validate_file_fraction(ctx, param, value):
    if value <= 0 or value > 1:
        raise click.BadParameter(f'must be a number > 0 and <= 1.')
    return value


def validate_count(ctx, param, value):
    if value <= 0:
        raise click.BadParameter(f'must be a number > 0.')
    return value


def validate_seed(ctx, param, value):
    if value is not None and (value < 0 or value > (2**32 - 1)):
        raise click.BadParameter(f'must be between 0 and 2**32 - 1.')
    return value


@evt_cmd.command('sample')
@click.option('-o', '--outfile', type=click.Path(), required=True,
    help='Output file path. ".gz" extension will gzip output.')
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_count,
    help='Number of events to keep, before noise filtering.')
@click.option('-f', '--file-fraction', type=float, default=0.1, show_default=True, callback=validate_file_fraction,
    help='Fraction of files to sample from.')
@click.option('--min-chl', type=int, default=0, show_default=True,
    help='Mininum chlorophyll (small) value.')
@click.option('--min-fsc', type=int, default=0, show_default=True,
    help='Mininum forward scatter (small) value.')
@click.option('--min-pe', type=int, default=0, show_default=True,
    help='Mininum phycoerythrin value.')
@click.option('-n', '--noise-filter', 'filter_noise', is_flag=True, default=False, show_default=True,
    help='Apply noise filter before subsampling.')
@click.option('-s', '--seed', type=int, callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.option('-v', '--verbose', count=True,
    help='Show more information. Specify more than once to show more information.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_evt_cmd(outfile, count, file_fraction, min_chl, min_fsc, min_pe,
                   filter_noise, seed, verbose, files):
    """
    Sample a subset of rows in EVT files.

    Sample a subset of rows EVT files.
    The list of EVT files can should be file paths or directory paths
    which will be searched for EVT files.
    COUNT events will be randomly selected from all data.
    For speed, only a random subset of files will be sampled from.
    To reduce file size only the D1, D2, fsc_small, and chl_small columns are
    saved.
    The output of this command can be converted back to a normal EVT file
    with zeros written in missing columns with the "evt rehydrate" command.
    """
    # dirs to file paths, only keep EVT/OPP files
    files = seaflowfile.keep_evt_files(expand_file_list(files))
    try:
        df = sample.sample(
            files, count, file_fraction, filter_noise=filter_noise,
            min_chl=min_chl, min_fsc=min_fsc, min_pe=min_pe, seed=seed,
            verbose=verbose
        )
    except (ValueError, IOError) as e:
        raise click.ClickException(str(e))
    try:
        fileio.write_labview(df, outfile)
    except (IOError, OSError) as e:
        raise click.ClickException("Could not write output file: {}".format(str(e)))


@evt_cmd.command('validate')
@click.option('-H', '--no-header', is_flag=True, default=False, show_default=True,
    help="Don't print column headers.")
@click.option('-S', '--no-summary', is_flag=True, default=False, show_default=True,
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

    # dirs to file paths
    files = expand_file_list(files)

    header_printed = False
    ok, bad = 0, 0

    for filepath in files:
        # Default values
        filetype = '-'
        file_id = '-'
        events = 0

        try:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
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


def expand_file_list(files_and_dirs):
    """Convert directories in file list to EVT/OPP file paths."""
    # Find files in directories
    dirs = [f for f in files_and_dirs if os.path.isdir(f)]
    files = [f for f in files_and_dirs if os.path.isfile(f)]

    dfiles = []
    for d in dirs:
        evt_files = seaflowfile.find_evt_files(d)
        opp_files = seaflowfile.find_evt_files(d, opp=True)
        dfiles = dfiles + evt_files + opp_files

    return files + dfiles
