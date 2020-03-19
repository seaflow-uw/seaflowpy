import os
import sys

import click
import pandas as pd
from seaflowpy import beads
from seaflowpy import errors
from seaflowpy import seaflowfile
from seaflowpy import fileio
from seaflowpy import sample
from seaflowpy import util


def validate_file_fraction(ctx, param, value):
    if value <= 0 or value > 1:
        raise click.BadParameter(f'must be a number > 0 and <= 1.')
    return value


def validate_positive(ctx, param, value):
    if value is not None and value <= 0:
        raise click.BadParameter(f'must be a number > 0.')
    return value


def validate_seed(ctx, param, value):
    if value is not None and (value < 0 or value > (2**32 - 1)):
        raise click.BadParameter(f'must be between 0 and 2**32 - 1.')
    return value


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
    Because of this, there may be files where "evt validate" reports 0 events
    while this tool reports > 0 events.

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

        # Try to parse filename as SeaFlow file
        try:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
            if sff.is_opp:
                filetype = 'opp'
            else:
                filetype = 'evt'
        except errors.FileError:
            # Might have unusual name
            pass
        try:
            events = fileio.read_labview_row_count(filepath)
        except errors.FileError:
            pass  # accept defaults, do nothing

        if not header_printed and not no_header:
            print('\t'.join(['path', 'file_id', 'type', 'events']))
            header_printed = True
        print('\t'.join([filepath, file_id, filetype, str(events)]))


@evt_cmd.command('beads')
@click.option('-f', '--frac', type=float, default=0.33, show_default=True,
    help='min_cluster_frac parameter to hdbscan. Min fraction of data which should be in cluster.')
@click.option('-o', '--outpath', type=click.Path(),
    help='Output file path for bead coordinates. Parquet format.')
@click.option('-O', '--other-params', type=click.Path(exists=True),
    help='Filtering parameter csv file to compare against')
@click.option('-p', '--plot-dir', type=click.Path(),
    help='Output directory for bead finding diagnostic plots. PNG format.')
@click.option('-P', '--pe-min', type=int, default=40000, show_default=True,
    help='PE minimum cutoff to use during bead cluster detection.')
@click.option('-r', '--radius', type=int, callback=validate_positive,
    help='Radius of circle used to collect bead locations.')
@click.option('-s', '--serial',
    help='Instrument serial number.')
@click.argument('evt-files', nargs=-1, type=click.Path(exists=True))
def beads_evt_cmd(frac, outpath, other_params, plot_dir, pe_min, radius, evt_files, serial):
    """
    Find bead location and generate filtering parameters.
    """
    if not evt_files:
        return

    # dirs to file paths
    files = seaflowfile.keep_evt_files(expand_file_list(evt_files))

    if other_params:
        try:
            params = fileio.read_filter_params_csv(other_params)
        except errors.FileError as e:
            raise click.ClickException(str(e))
        otherip = beads.params2ip(params)
    else:
        otherip = None

    coords_df = None
    etemplate = "{}: {!r}: {}"
    for f in files:
        try:
            results = beads.find_beads(f, serial, pe_min=pe_min, min_cluster_frac=frac)
            df = results["inflection_point"]
            df["path"] = results["evt_path"]
            df["file_id"] = seaflowfile.SeaFlowFile(f).file_id
            df["date"] = seaflowfile.SeaFlowFile(f).date
            if coords_df is None:
                coords_df = df
            else:
                coords_df = pd.concat([coords_df, df], ignore_index=True)
        except Exception as e:
            print(etemplate.format(type(e).__name__, e.args, str(e), file=sys.stderr))
            continue
        if plot_dir:
            sff = seaflowfile.SeaFlowFile(f)
            plot_path = os.path.join(plot_dir, sff.file_id + ".png")
            util.mkdir_p(os.path.dirname(plot_path))
            try:
                beads.plot(results, plot_path, otherip=otherip)
            except Exception as e:
                print(etemplate.format(type(e).__name__, e.args, str(e), file=sys.stderr))
    coords_df.to_parquet(outpath)


@evt_cmd.command('sample')
@click.option('-o', '--outpath', type=click.Path(), required=True,
    help="""Output path. If --multi is used, this option will be interpreted as a
            directory and there will be one output file per input file. All files will
            gzip compressed. If --multi is not used all input files will be subsampled
            into one output file given by this option. If --outpath ends with '.gz'
            the output file will be gzip compressed.""")
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_positive,
    help='Target number of events to keep.')
@click.option('-f', '--file-fraction', type=float, default=0.1, show_default=True, callback=validate_file_fraction,
    help='Fraction of files to sample from, > 0 and <= 1.')
@click.option('--min-chl', type=int, default=0, show_default=True,
    help='Mininum chlorophyll (small) value.')
@click.option('--min-fsc', type=int, default=0, show_default=True,
    help='Mininum forward scatter (small) value.')
@click.option('--min-pe', type=int, default=0, show_default=True,
    help='Mininum phycoerythrin value.')
@click.option('--min-date', type=str,
    help='Minimum date of file to sample.')
@click.option('--max-date', type=str,
    help='Maximum date of file to sample.')
@click.option('--multi', is_flag=True, default=False, show_default=True,
    help='Sample each input file separately and create one output file per input file.')
@click.option('-n', '--noise-filter', is_flag=True, default=False, show_default=True,
    help='Apply noise filter before subsampling.')
@click.option('-p', '--process-count', type=int, default=1, show_default=True, callback=validate_positive,
    help='Number of processes to use.')
@click.option('-s', '--seed', type=int, callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.option('-v', '--verbose', count=True,
    help='Show more information. Specify more than once to show more information.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_evt_cmd(outpath, count, file_fraction, min_chl, min_fsc, min_pe,
                   min_date, max_date, multi, noise_filter, process_count, seed,
                   verbose, files):
    """
    Sample a subset of events in EVT files.

    The list of EVT files can be file paths or directory paths
    which will be searched for EVT files.
    COUNT events will be randomly selected from all data.
    If --outpath is a single file only a fraction of the input files will be
    sampled from (FILE-FRACTION) and one combined output file will be created.
    """
    # dirs to file paths, only keep EVT/OPP files
    files = seaflowfile.keep_evt_files(expand_file_list(files))
    time_files = seaflowfile.timeselect_evt_files(files, min_date, max_date)
    chosen_files = sample.random_select(time_files, file_fraction, seed)

    results, errs = sample.sample(
        chosen_files,
        count,
        outpath,
        min_chl=min_chl,
        min_fsc=min_fsc,
        min_pe=min_pe,
        multi=multi,
        noise_filter=noise_filter,
        process_count=process_count,
        seed=seed
    )

    printed = False
    if verbose:
        print("\t".join(["file_ID", "events", "postfilter_events", "sampled_events", "message"]), file=sys.stderr)
        for r in results:
            vals = [r["file_id"], r["events"], r["events_postfilter"], r["events_postsampling"], r["msg"]]
            print("\t".join([str(v) for v in vals]), file=sys.stderr)
        printed = True
    else:
        # Print only files that couldn't be read
        for r in results:
            if r["msg"]:
                print("\t".join([r["file_id"], r["msg"]]), file=sys.stderr)
                printed = True
    if printed:
        print("", file=sys.stderr)

    if errs:
        print("Errors encountered", file=sys.stderr)
        for err in errs:
            print(err, file=sys.stderr)
        print("", file=sys.stderr)

    print("{} input files".format(len(files)), file=sys.stderr)
    print("{} selected files".format(len(chosen_files)), file=sys.stderr)
    print("{} total events".format(sum([r["events"] for r in results])), file=sys.stderr)
    print("{} events after noise/min filtering".format(sum([r["events_postfilter"] for r in results])), file=sys.stderr)
    print("{} events sampled".format(sum([r["events_postsampling"] for r in results])), file=sys.stderr)


@evt_cmd.command('validate')
@click.option('-a', '--all', 'report_all', is_flag=True,
    help='Show information for all files. If not specified then only files errors are printed.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def validate_evt_cmd(report_all, files):
    """
    Examines EVT/OPP files.

    If any of the file arguments are directories all EVT/OPP files within those
    directories will be recursively found and examined. Prints file validation
    report to STDOUT. Print summary of files passing validation to STDERR.
    """
    if not files:
        return

    # dirs to file paths
    files = expand_file_list(files)

    header_printed = False
    ok, bad = 0, 0

    for filepath in files:
        # Default values
        type_from_filename = '-'
        filetype = '-'
        file_id = '-'
        events = 0

        # Try to parse filename as SeaFlow file
        try:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
            if sff.is_evt:
                type_from_filename = 'evt'
                filetype = 'evt'
            elif sff.is_opp:
                type_from_filename = 'opp'
                filetype = 'opp'
        except errors.FileError:
            # unusual name, no file_id
            pass

        if type_from_filename == 'evt':
            try:
                data = fileio.read_evt_labview(filepath)
                status = 'OK'
                ok += 1
                events = len(data.index)
            except errors.FileError as e:
                status = str(e)
                bad += 1
                events = 0
        elif type_from_filename == 'opp':
            try:
                data = fileio.read_opp_labview(filepath)
                status = 'OK'
                ok += 1
                events = len(data.index)
            except errors.FileError as e:
                status = str(e)
                bad += 1
                events = 0
        elif type_from_filename == '-':
            # Try to read as both EVT or OPP
            try:
                data = fileio.read_evt_labview(filepath)
                filetype = 'evt'
                status = 'OK'
                ok += 1
                events = len(data.index)
            except errors.FileError:
                try:
                    data = fileio.read_opp_labview(filepath)
                    filetype = 'opp'
                    status = 'OK'
                    ok += 1
                    events = len(data.index)
                except errors.FileError as e:
                    status = str(e)
                    bad += 1
                    events = 0

        if not header_printed:
            print('\t'.join(['path', 'file_id', 'type', 'status', 'events']))
            header_printed = True
        if (report_all and status == 'OK') or (status != 'OK'):
            print('\t'.join([filepath, file_id, filetype, status, str(events)]))
    print('%d/%d files passed validation' % (ok, bad + ok), file=sys.stderr)


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
