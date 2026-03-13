import datetime
import pathlib
import sys

import click
import pandas as pd

from seaflowpy import seaflowfile
from seaflowpy import time
from seaflowpy import util


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
        except ValueError as e:
            raise click.BadParameter('unable to parse timestamp.') from e
    return value

def validate_hours(ctx, param, value):
    if value is not None:
        if value <= 0:
            raise click.BadParameter('hours must be > 0.')
    return value


@click.group()
def opp_cmd():
    """OPP file subcommand."""
    pass


@opp_cmd.command('sample')
@click.option('-o', '--outpath', type=click.Path(path_type=pathlib.Path), required=True,
    help="""Output path for parquet file with subsampled event data.""")
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_positive,
    help='Target number of events to keep.')
@click.option('--min-date', type=str, callback=validate_timestamp,
    help='Minimum date of file to sample as ISO8601 timestamp.')
@click.option('--max-date', type=str, callback=validate_timestamp,
    help='Maximum date of file to sample as ISO8601 timestamp.')
@click.option('--tail-hours', type=int, metavar='N', callback=validate_hours,
    help="""Only subsample the most recent N hours of data. Unsets --max-date.
            If --min-date is also provided it will be used if it is more recent
            than <last date - N hours>.""")
@click.option('-s', '--seed', callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_opp_cmd(outpath, count, min_date, max_date, tail_hours, seed,
                   files):
    """
    Sample a subset of events in OPP files.

    The list of OPP files can be file paths or directory paths
    which will be searched for OPP files.
    COUNT events will be randomly selected from all data.
    """
    files = sorted(seaflowfile.keep_opp_files(util.expand_file_list(files)))
    df = pd.DataFrame()
    sub = pd.DataFrame()
    if files:
        timestamps = [pathlib.Path(f).name.split(".")[0] for f in files]
        timestamps = [seaflowfile.timestamp_from_filename(ts) for ts in timestamps]
        timestamps = [datetime.datetime.fromisoformat(ts) for ts in timestamps]
        files_df = pd.DataFrame({"path": files, "date": timestamps})

        # Select by time.
        if tail_hours is not None and len(files_df):
            # Set min_date based on tail_hours from latest date
            opp = pd.read_parquet(files_df.path.values[-1], columns=["date"])
            tail_min_date = opp.date.max() - datetime.timedelta(hours=tail_hours)
            if min_date is None or min_date < tail_min_date:
                min_date = tail_min_date
            max_date = None

        if min_date is not None:
            files_df = files_df[files_df.date + datetime.timedelta(hours=1) >= min_date]
        if max_date is not None:
            files_df = files_df[files_df.date <= max_date]
        
        if len(files_df):
            outpath.parent.mkdir(parents=True, exist_ok=True)
            try:
                opp_dfs = [pd.read_parquet(f) for f in files_df.path]
            except Exception as e:
                print("Error reading OPP files: {}".format(e), file=sys.stderr)
                return
            df = pd.concat(opp_dfs, ignore_index=True)
            if min_date is not None:
                df = df[df.date >= min_date]
            if max_date is not None:
                df = df[df.date <= max_date]
            count = min(count, len(df))
            if seed is not None:
                sub = df.sample(n=count, random_state=seed)
            else:
                sub = df.sample(n=count)
            sub.to_parquet(outpath)

    print("{} particles in time range".format(len(df)), file=sys.stderr)
    if len(sub):
        print("{} particles selected with time range {} - {} ".format(len(sub), sub.date.min().isoformat(), sub.date.max().isoformat()), file=sys.stderr)


@opp_cmd.command('sample-hourly')
@click.option('-o', '--outdir', type=click.Path(path_type=pathlib.Path), required=True,
    help="""Output directory for per-file parquet files with subsampled event data.""")
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_positive,
    help='Target number of events to keep per file.')
@click.option('--min-date', type=str, callback=validate_timestamp,
    help='Minimum date of file to sample as ISO8601 timestamp.')
@click.option('--max-date', type=str, callback=validate_timestamp,
    help='Maximum date of file to sample as ISO8601 timestamp.')
@click.option('-s', '--seed', callback=validate_seed,
    help='Integer seed for PRNG, otherwise system-dependent source of randomness is used to seed the PRNG.')
@click.argument('files', nargs=-1, type=click.Path(exists=True))
def sample_hourly_opp_cmd(outdir, count, min_date, max_date, seed, files):
    """
    Sample a subset of events in OPP files, outputting one file per source file.

    The list of OPP files can be file paths or directory paths
    which will be searched for OPP files. OPP files are already binned by hour,
    so each output file corresponds to one input file and uses the same filename.
    COUNT events will be randomly selected from each input file.

    If an output file already exists and its file_id set matches the source
    file's file_id set, that file is skipped (smart caching).
    """
    files = sorted(seaflowfile.keep_opp_files(util.expand_file_list(files)))
    if not files:
        print("0 input files", file=sys.stderr)
        return

    timestamps = [pathlib.Path(f).name.split(".")[0] for f in files]
    timestamps = [seaflowfile.timestamp_from_filename(ts) for ts in timestamps]
    timestamps = [datetime.datetime.fromisoformat(ts) for ts in timestamps]
    files_df = pd.DataFrame({"path": files, "date": timestamps})

    if min_date is not None:
        files_df = files_df[files_df.date >= min_date]
    if max_date is not None:
        files_df = files_df[files_df.date <= max_date]

    outdir.mkdir(parents=True, exist_ok=True)

    files_skipped = 0
    files_sampled = 0
    particles_sampled = 0

    for row in files_df.itertuples(index=False):
        src_path = str(row.path)
        outfile = outdir / pathlib.Path(src_path).name

        if outfile.exists():
            try:
                old_ids = set(pd.read_parquet(outfile, columns=["file_id"])["file_id"].unique())
                src_ids = set(pd.read_parquet(src_path, columns=["file_id"])["file_id"].unique())
            except Exception as e:
                print("Error checking cache for {}: {}".format(src_path, e), file=sys.stderr)
            else:
                if old_ids == src_ids:
                    files_skipped += 1
                    continue

        try:
            src_df = pd.read_parquet(src_path)
        except Exception as e:
            print("Error reading {}: {}".format(src_path, e), file=sys.stderr)
            continue

        n = min(count, len(src_df))
        if seed is not None:
            sub = src_df.sample(n=n, random_state=seed)
        else:
            sub = src_df.sample(n=n)
        sub.to_parquet(outfile)
        files_sampled += 1
        particles_sampled += len(sub)

    print("{} input files".format(len(files)), file=sys.stderr)
    print("{} files within time window".format(len(files_df)), file=sys.stderr)
    print("{} files skipped (already up-to-date)".format(files_skipped), file=sys.stderr)
    print("{} files sampled".format(files_sampled), file=sys.stderr)
    print("{} particles sampled".format(particles_sampled), file=sys.stderr)
