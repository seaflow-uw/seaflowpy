import datetime
import pathlib
import sys

import click
import pandas as pd

from seaflowpy import seaflowfile
from seaflowpy import time


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
    files = sorted(seaflowfile.expand_file_list(files))
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

        outpath.parent.mkdir(parents=True, exist_ok=True)

        df = pd.concat([pd.read_parquet(f) for f in files_df.path], ignore_index=True)
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
        print("{} particles selected with time range {} - {} ".format(len(sub), sub.date.min().isoformat(), sub.date.max().isoformat()), file=sys.stderr)
    else:
        print("0 particles in time range", file=sys.stderr)
