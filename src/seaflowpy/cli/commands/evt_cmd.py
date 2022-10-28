import datetime
import logging
import os
import pathlib
import sys

import click
from seaflowpy import errors
from seaflowpy import seaflowfile
from seaflowpy import fileio
from seaflowpy import sample
from seaflowpy import sfl
from seaflowpy import time


def validate_file_fraction(ctx, param, value):
    if value <= 0 or value > 1:
        raise click.BadParameter('must be a number > 0 and <= 1.')
    return value


def validate_positive(ctx, param, value):
    if value is not None and value <= 0:
        raise click.BadParameter('must be a number > 0.')
    return value


def validate_seed(ctx, param, value):
    if value is not None:
        try:
            value = int(value)
        except ValueError as e:
            raise click.BadParameter('must be an integer: {}.'.format(e))
        if (value < 0 or value > (2**32 - 1)):
            raise click.BadParameter('must be between 0 and 2**32 - 1.')
    return value


def validate_timestamp(ctx, param, value):
    if value is not None:
        try:
            value = time.parse_date(value, assume_utc=False)
        except ValueError:
            raise click.BadParameter('unable to parse timestamp.')
    return value

def validate_hours(ctx, param, value):
    if value is not None:
        if value <= 0:
            raise click.BadParameter('hours must be > 0.')
    return value


@click.group()
def evt_cmd():
    """EVT file subcommand."""
    pass


@evt_cmd.command('sample')
@click.option('-o', '--outpath', type=click.Path(), required=True,
    help="""Output path for parquet file with subsampled event data.""")
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_positive,
    help='Target number of events to keep.')
@click.option('-f', '--file-fraction', type=float, default=0.1, show_default=True, callback=validate_file_fraction,
    help='Fraction of files to sample from, > 0 and <= 1. Using --multi sets this option to 1.')
@click.option('--min-chl', type=int, default=0, show_default=True,
    help='Mininum chlorophyll (small) value.')
@click.option('--min-fsc', type=int, default=0, show_default=True,
    help='Mininum forward scatter (small) value.')
@click.option('--min-pe', type=int, default=0, show_default=True,
    help='Mininum phycoerythrin value.')
@click.option('--min-date', type=str, callback=validate_timestamp,
    help='Minimum date of file to sample as ISO8601 timestamp.')
@click.option('--max-date', type=str, callback=validate_timestamp,
    help='Maximum date of file to sample as ISO8601 timestamp.')
@click.option('--tail-hours', type=int, metavar='N', callback=validate_hours,
    help="""Only subsample the most recent N hours of data. Unsets --max-date.
            If --min-date is also provided it will be used if it is more recent
            than <last date - N hours>.""")
@click.option('--multi', is_flag=True, default=False, show_default=True,
    help='Sample --count events from each input file separately, rather than --count events overall.')
@click.option('-n', '--noise-filter', is_flag=True, default=False, show_default=True,
    help='Apply noise filter before subsampling.')
@click.option('-p', '--process-count', type=int, default=1, show_default=True, callback=validate_positive,
    help='Number of processes to use.')
@click.option('--saturation-filter', is_flag=True, default=False, show_default=True,
    help='Apply saturation filter before subsampling.')
@click.option('-s', '--seed', callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.option('-S', '--sfl', 'sfl_path', type=click.Path(),
    help="""SFL file that can be used to associate dates with EVT files. Useful when
            sampling undated EVT files.""")
@click.option('-v', '--verbose', count=True,
    help='Show more information. Specify more than once to show more information.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_evt_cmd(outpath, count, file_fraction, min_chl, min_fsc, min_pe,
                   min_date, max_date, tail_hours, multi, noise_filter, process_count, 
                   saturation_filter, seed, sfl_path, verbose, files):
    """
    Sample a subset of events in EVT files.

    The list of EVT files can be file paths or directory paths
    which will be searched for EVT files.
    COUNT events will be randomly selected from all data.
    """
    if verbose == 0:
        loglevel = logging.WARNING
    elif verbose == 1:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(message)s", level=loglevel)

    # Get file to date mappings from SFL file
    files = seaflowfile.keep_evt_files(expand_file_list(files))
    files = seaflowfile.sorted_files(files)
    if sfl_path:
        sfl_df = sfl.read_file(sfl_path, convert_dates=True)
        sfl_df = sfl.fix(sfl_df)  # ensure valid file_ids in file column
    else:
        sfl_df = None
    evt = seaflowfile.date_evt_files(files, sfl_df)

    # Select by time.
    # If paths don't have timestamps and SFL not provided, this step will always
    # filter out all files.
    if tail_hours is not None and len(evt):
        tail_min_date = evt.date.max() - datetime.timedelta(hours=tail_hours)
        if min_date is None or min_date < tail_min_date:
            min_date = tail_min_date
        max_date = None
    if min_date is not None:
        evt = evt[evt.date >= min_date]
    if max_date is not None:
        evt = evt[evt.date <= max_date]

    # Select fraction of files
    if not multi:
        chosen_files = sample.random_select(list(evt.path), file_fraction, seed)
    else:
        chosen_files = list(evt.path)

    outdir = os.path.dirname(outpath)
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)

    results, errs = sample.sample(
        chosen_files,
        count,
        outpath,
        dates=dict(zip(list(evt.file_id), list(evt.date))),
        min_chl=min_chl,
        min_fsc=min_fsc,
        min_pe=min_pe,
        multi=multi,
        noise_filter=noise_filter,
        saturation_filter=saturation_filter,
        process_count=process_count,
        seed=seed
    )

    printed = False
    if verbose:
        if len(results):
            print("\t".join(["file_ID", "events", "postfilter_events", "sampled_events", "message"]), file=sys.stderr)
        for r in results:
            vals = [r["file_id"], r["events"], r["events_postfilter"], r["events_postsampling"], r["msg"]]
            print("\t".join([str(v) for v in vals]), file=sys.stderr)
        printed = True
    else:
        # Print only files that couldn't be read
        if len(results):
            print("\t".join(["file_ID", "events", "postfilter_events", "sampled_events", "message"]), file=sys.stderr)
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

    if sfl_path:
        print("{} entries found in SFL file".format(len(sfl_df)), file=sys.stderr)
    print("{} input files".format(len(files)), file=sys.stderr)
    print("{} files within time window".format(len(evt)), file=sys.stderr)
    print("{} selected files".format(len(chosen_files)), file=sys.stderr)
    print("{} total events".format(sum([r["events"] for r in results])), file=sys.stderr)
    print("{} events after noise/sat/min filtering".format(sum([r["events_postfilter"] for r in results])), file=sys.stderr)
    print("{} events sampled".format(sum([r["events_postsampling"] for r in results])), file=sys.stderr)



@evt_cmd.command('dates')
@click.option('--min-date', type=str, callback=validate_timestamp,
    help='Minimum date of file to consider.')
@click.option('--max-date', type=str, callback=validate_timestamp,
    help='Maximum date of file to consider.')
@click.option('--tail-hours', type=int, metavar='N', callback=validate_hours,
    help="""Only consider the most recent N hours of data. Unsets --max-date.
            If --min-date is also provided it will be used if it is more recent
            than <last date - N hours>.""")
@click.option('-S', '--sfl', 'sfl_path', type=click.Path(),
    help="""SFL file that can be used to associate dates with EVT files. Useful when
            sampling undated EVT files.""")
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def dates_evt_cmd(min_date, max_date, tail_hours, sfl_path, files):
    """
    Get date range for a set of EVT files.

    The list of EVT files can be file paths or directory paths
    which will be searched for EVT files.
    """
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(message)s", level=logging.INFO)
    files = seaflowfile.keep_evt_files(expand_file_list(files))
    files = seaflowfile.sorted_files(files)
    if sfl_path:
        sfl_df = sfl.read_file(sfl_path, convert_dates=True)
        sfl_df = sfl.fix(sfl_df)  # ensure valid file_ids in file column
    else:
        sfl_df = None
    evt = seaflowfile.date_evt_files(files, sfl_df)

    # Select by time.
    # If paths don't have timestamps and SFL not provided, this step will always
    # filter out all files.
    if tail_hours is not None and len(evt):
        tail_min_date = evt.date.max() - datetime.timedelta(hours=tail_hours)
        if min_date is None or min_date < tail_min_date:
            min_date = tail_min_date
        max_date = None
    if min_date is not None:
        evt = evt[evt.date >= min_date]
    if max_date is not None:
        evt = evt[evt.date <= max_date]

    if len(evt):
        print(f"{evt.date.min().isoformat()} {evt.date.max().isoformat()}")


@evt_cmd.command('validate')
@click.option('-a', '--all', 'report_all', is_flag=True,
    help='Show information for all files. If not specified then only files errors are printed.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def validate_evt_cmd(report_all, files):
    """
    Examines EVT files.

    If any of the file arguments are directories all EVT files within those
    directories will be recursively found and examined. Prints file validation
    report to STDOUT. Print summary of files passing validation to STDERR.
    """
    # TODO: expand to calculate hash for binary EVT
    # and OPP parquet files with an extra flag. OPP parquet should produce hashes for
    # each file_id in the whole file (e.g. by group_by)
    if not files:
        return

    # dirs to file paths
    files = expand_file_list(files)

    header_printed = False
    ok, bad = 0, 0

    for filepath in files:
        # Default values
        version = '-'
        file_id = '-'
        events = 0

        # Try to parse filename as SeaFlow file
        try:
            sff = seaflowfile.SeaFlowFile(filepath)
            file_id = sff.file_id
        except errors.FileError:
            # unusual name, no file_id
            pass

        # Try to read file as binary EVT
        try:
            data = fileio.read_evt_labview(filepath)
            version = data['version']
            status = 'OK'
            ok += 1
            events = len(data['df'].index)
        except errors.FileError as e:
            status = str(e)
            bad += 1

        if not header_printed:
            print('\t'.join(['path', 'file_id', 'version', 'status', 'events']))
            header_printed = True
        if (report_all and status == 'OK') or (status != 'OK'):
            print('\t'.join([filepath, file_id, version, status, str(events)]))
    print('%d/%d files passed validation' % (ok, bad + ok), file=sys.stderr)


def expand_file_list(files_and_dirs):
    """Convert directories in file list to EVT file paths."""
    # Find files in directories
    dirs = [f for f in files_and_dirs if os.path.isdir(f)]
    files = [f for f in files_and_dirs if os.path.isfile(f)]

    dfiles = []
    for d in dirs:
        dfiles = dfiles + seaflowfile.find_evt_files(d)

    return files + dfiles
