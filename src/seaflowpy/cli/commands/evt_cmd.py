import datetime
import logging
import os
import pathlib
import sys

import click
import numpy as np
import pandas as pd
import pkg_resources
from seaflowpy import beads
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


@evt_cmd.command('beads')
@click.option('-c', '--cruise', type=str, required=True,
    help='Cruise name for summary plot title.')
@click.option('-C', '--cytograms', is_flag=True, default=False, show_default=True,
    help="""Create per-time-window cytogram PNGs. Files may be about 1-2MB and plotting
    is much slower than bead finding itself, so consider using with a limited time
    range and/or large resolution for diagnostic purposes.""")
@click.option('-e', '--event-limit', type=int, default=30000, show_default=True,
    help='Maximum event count for bead clustering.')
@click.option('-f', '--frac', type=float, default=0.33, show_default=True,
    help='min_cluster_frac parameter to hdbscan. Min fraction of data which should be in cluster.')
@click.option("-i", "--iqr", type=int, default=3000, show_default=True,
    help='Maximum interquartile spread to accept a bead location for fsc_small, D1, D2.')
@click.option('--min-date', type=str, callback=validate_timestamp,
    help='Minimum date of file to sample as ISO8601 timestamp.')
@click.option('--max-date', type=str, callback=validate_timestamp,
    help='Maximum date of file to sample as ISO8601 timestamp.')
@click.option('--min-fsc', type=int, default=45000, show_default=True,
    help='FSC minimum cutoff to use during bead cluster detection.')
@click.option('--min-pe', type=int, default=47500, show_default=True,
    help='PE minimum cutoff to use during bead cluster detection.')
@click.option('-o', '--out-dir', type=click.Path(), required=True,
    help='Directory for output files.')
@click.option('-O', '--other-params', type=click.Path(exists=True),
    help='Filtering parameter csv file to compare against')
@click.option('-r', '--resolution', type=str, default='1H', show_default=True,
    help='Time resolution for bead detection. Follows Pandas offset aliases.')
@click.option('-v', '--verbose', count=True,
    help='Print progress info.')
@click.argument('particle-file', nargs=1, type=click.Path(exists=True))
def beads_evt_cmd(cruise, cytograms, event_limit, frac, iqr, min_date,
    max_date, min_fsc, min_pe, out_dir, other_params, resolution, verbose,
    particle_file):
    """
    Find bead location and generate filtering parameters.
    """
    if verbose == 0:
        loglevel = logging.WARNING
    elif verbose == 1:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG
    logging.basicConfig(format="%(asctime)s:%(levelname)s: %(message)s", level=loglevel)

    logging.info("finding beads in cruise %s", cruise)
    logging.info(
        "version=%s resolution=%s event-limit=%d frac=%f fsc-min=%d pe-min=%d iqr=%d",
        pkg_resources.get_distribution("seaflowpy").version,
        resolution,
        event_limit,
        frac,
        min_fsc,
        min_pe,
        iqr
    )
    logging.info("writing results to %s", out_dir)

    if other_params:
        logging.info("other filter parameter file: %s", other_params)
        try:
            params = fileio.read_filter_params_csv(other_params)
        except errors.FileError as e:
            raise click.ClickException(str(e))
        otherip = beads.params2ip(params)
    else:
        otherip = None

    evt_df = pd.read_parquet(particle_file)
    if len(evt_df) == 0:
        raise click.ClickException("no EVT data for bead finding")
    if "date" not in evt_df.columns:
        evt_df = evt_df.reset_index()  # maybe it's the index, don't drop
    if "date" not in evt_df.columns:
        raise click.ClickException("no date column in EVT dataframe")
    logging.info("%d particles in %s", len(evt_df), particle_file)

    # Apply any date filters
    if min_date or max_date:
        date_bool = np.full(len(evt_df), True, dtype=bool)
        logging.info("apply date filter, %s to %s", min_date, max_date)
        if min_date is not None:
            date_bool = evt_df["date"] >= min_date
            logging.info("%s after min_date filter", np.sum(date_bool))
        if max_date is not None:
            date_bool = date_bool & (evt_df["date"] <= max_date)
            logging.info("%s after max_date filter", np.sum(date_bool))
        evt_df = evt_df[date_bool]
        logging.info(
            "%s rows between %s and %s after date filter",
            len(evt_df), evt_df["date"].min(), evt_df["date"].max()
        )

    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    cyto_plot_dir = os.path.join(out_dir, "cytogram_plots")
    summary_plot_path = os.path.join(out_dir, f"{cruise}.summary.png")

    all_dfs = []
    for name, group in evt_df.set_index("date").resample(resolution):
        if len(group) == 0:
            continue
        if len(group) <= event_limit:
            tmp_df = group.reset_index(drop=True)
            logging.info("clustering %s (%d events)", str(name), len(group))
        else:
            tmp_df = group.reset_index(drop=True).sample(n=event_limit, random_state=12345).reset_index(drop=True)
            logging.info("clustering %s (%d events reduced to %d)", str(name), len(group), len(tmp_df))
        try:
            results = beads.find_beads(
                tmp_df,
                min_cluster_frac=frac,
                min_fsc=min_fsc,
                min_pe=min_pe
            )
        except Exception as e:
            logging.warning("%s: %s", type(e).__name__, str(e))
            if type(e).__name__ != "ClusterError":
                raise e
        else:
            if results["message"]:
                logging.warning("%s", results["message"])
            df = results["bead_coordinates"]
            df["date"] = name
            all_dfs.append(df)

        if cytograms:
            logging.info("plotting   %s", str(name))  # space intentional to line up with "clustering ...."

            pathlib.Path(cyto_plot_dir).mkdir(parents=True, exist_ok=True)
            cyto_plot_path = os.path.join(cyto_plot_dir, name.isoformat().replace(":", "-"))
            try:
                beads.plot(results, cyto_plot_path, file_id=name, otherip=otherip)
            except Exception as e:
                logging.warning("%s: %s", type(e).__name__, str(e))

    if all_dfs:
        out_df = pd.concat(all_dfs, ignore_index=True)
        out_df["resolution"] = resolution
        out_df["resolution"] = out_df["resolution"].astype("category")
        parquet_path = os.path.join(out_dir, cruise + f".beads-by-{resolution}" + ".parquet")

        logging.info("writing bead position parquet %s", parquet_path)
        out_df.to_parquet(parquet_path)
        logging.info("creating summary plot")
        beads.plot_cruise(
            out_df,
            summary_plot_path,
            filter_params_path=other_params,
            cruise=cruise,
            iqr=iqr
        )
    logging.info("done")


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
@click.option('-s', '--seed', callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.option('-S', '--sfl', 'sfl_path', type=click.Path(),
    help="""SFL file that can be used to associate dates with EVT files. Useful when
            sampling undated EVT files.""")
@click.option('-v', '--verbose', count=True,
    help='Show more information. Specify more than once to show more information.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_evt_cmd(outpath, count, file_fraction, min_chl, min_fsc, min_pe,
                   min_date, max_date, tail_hours, multi, noise_filter, process_count, seed,
                   sfl_path, verbose, files):
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
    print("{} events after noise/min filtering".format(sum([r["events_postfilter"] for r in results])), file=sys.stderr)
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
