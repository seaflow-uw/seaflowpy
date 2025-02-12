import datetime
import logging
import pathlib
import sys
from typing import Any, cast
from functools import partial

import click
import pandas as pd
from joblib import Parallel, delayed
from seaflowpy import errors
from seaflowpy import seaflowfile
from seaflowpy import fileio
from seaflowpy import particleops
from seaflowpy import sample
from seaflowpy import sfl
from seaflowpy import time
from seaflowpy import util
from tqdm import tqdm


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
@click.option('-o', '--outpath', type=click.Path(path_type=pathlib.Path), required=True,
    help="""Output path for parquet file with subsampled event data.""")
@click.option('-c', '--count', type=int, default=100000, show_default=True, callback=validate_positive,
    help='Target number of events to keep.')
@click.option('--cutoffs-file', type=click.Path(readable=True, path_type=pathlib.Path))
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
def sample_evt_cmd(outpath, cutoffs_file, count, file_fraction, min_chl, min_fsc, min_pe,
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

    # Read cutoffs file
    if cutoffs_file is not None:
        cutoffs = pd.read_csv(cutoffs_file, sep="\t")
        # Pylance 
        min_fsc = cast(int, cutoffs["min_fsc_small"].astype(int).iloc[0])
        min_pe = cast(int, cutoffs["min_pe"].astype(int).iloc[0])
        min_chl = cast(int, cutoffs["min_chl_small"].astype(int).iloc[0])
    logging.info(
        "effective cutoffs are, min_fsc_small=%d, min_pe=%d, min_chl_small=%d",
        min_fsc, min_pe, min_chl
    )

    # Get file to date mappings from SFL file
    files = seaflowfile.keep_evt_files(util.expand_file_list(files))
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

    outpath.parent.mkdir(parents=True, exist_ok=True)

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

    if sfl_df is not None:
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
@click.option('--sfl', 'sfl_path', type=click.Path(),
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
    files = seaflowfile.keep_evt_files(util.expand_file_list(files))
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
@click.option('--filter-by-name', is_flag=True,
    help="Exclude files that don't have EVT-like names.")
@click.option('--hash', 'hash_', is_flag=True,
    help='MD5 hash each EVT, with contents expressed as df.to_numpy().tobytes()')
@click.option('-n', '--n-jobs', default=1, type=int, help='worker jobs')
@click.option('-r', '--reduced-columns', is_flag=True,
    help=f'Hash on the reduced column set {particleops.REDUCED_COLUMNS}')
@click.option('-t', '--tsv', is_flag=True,
    help="Print TSV output to STDOUT. Default is more human-readable space-separated.")
@click.option('-p', '--progress', is_flag=True,
    help='Print progress')
@click.argument('paths', nargs=-1, type=click.Path(exists=True))
def validate_evt_cmd(
    report_all, filter_by_name, hash_, n_jobs, reduced_columns, tsv, progress,
    paths
):
    """
    Examines EVT files.

    If any of the file arguments are directories all EVT files within those
    directories will be recursively found and examined. Prints file validation
    report to STDOUT. Print summary of files passing validation to STDERR.
    """
    if not paths:
        return

    if reduced_columns:
        cols = particleops.REDUCED_COLUMNS
    else:
        cols = None

    # dirs to file paths
    files = util.expand_file_list(paths)
    if filter_by_name:
        files = seaflowfile.keep_evt_files(files)

    file_ids = []
    for filepath in files:
        # Try to parse filename as SeaFlow file
        try:
            sff = seaflowfile.SeaFlowFile(filepath)
        except errors.FileError:
            # unusual name, no file_id
            file_ids.append('-')
        else:
            file_ids.append(sff.file_id)
    work = pd.DataFrame({'id': file_ids, 'path': files})
    parallel = Parallel(n_jobs=max(1, n_jobs), return_as="generator_unordered")
    work_func = partial(_validate_evt_file, checksum=hash_, cols=cols)
    with tqdm(desc="files", total=len(work), file=sys.stderr) as bar:
        results_list = []
        for i, res in enumerate(parallel(delayed(work_func)(r[1]) for r in work.iterrows())):
            bar.update(1)
            if i % 10 == 0 and res is not None:
                bar.set_description(pathlib.Path(res["path"]).name)
            results_list.append(res)
        results = pd.DataFrame(results_list)
    results.sort_values(by='id', key=_idkey, inplace=True)

    if len(results):
        print(
            '%d/%d files passed validation' % (results['err'].isna().sum(), len(results)),
            file=sys.stderr
        )
    if report_all:
        results_output = results
    else:
        results_output = results.loc[~results['err'].isna()]

    if len(results_output):
        if tsv:
            results_output.to_csv(sys.stdout, sep="\t", index=False)
        else:
            results_output.to_string(index=False, buf=sys.stdout)

    print()


def _validate_evt_file(w: dict[str, Any], checksum=True, cols=None) -> dict[str, Any]:
    w = w.copy()
    data = fileio.validate_evt_file(w['path'], checksum=checksum, cols=cols)
    w['hash'] = data['hash']
    w['count'] = data['count']
    w['version'] = data['version']
    w['err'] = data['err']
    return w


def _idkey(id_series):
    """pandas.DataFrame.sort_values key callable"""
    def make_key(id_):
        try:
            sf = seaflowfile.SeaFlowFile(id_)
        except errors.FileError:
            return (0, 0, id_)
        else:
            return sf.sort_key

    return id_series.apply(make_key)


@evt_cmd.command('parquet')
@click.option('-n', '--n-jobs', default=1, type=int, help='worker jobs')
@click.option('-o', '--out-dir', default='.', type=click.Path(path_type=pathlib.Path),
    help='Output directory')
@click.option('-p', '--progress', is_flag=True,
    help='Print progress')
@click.option('--sfl', 'sfl_path', type=click.Path(),
    help="""SFL file that can be used to associate dates with EVT files.""")
@click.argument('paths', nargs=-1, type=click.Path(exists=True, path_type=str))
def parquet_cmd(n_jobs, out_dir, progress, paths, sfl_path):
    """
    Convert binary EVT files to reduced Parquet.

    Output files will be placed in appropriate day of year directories inside
    out-dir.
    """
    in_files = seaflowfile.keep_evt_files(util.expand_file_list(paths))
    if sfl_path:
        sfl_df = sfl.read_file(sfl_path, convert_dates=True)
        sfl_df = sfl.fix(sfl_df)  # ensure valid file_ids in file column
    else:
        sfl_df = None
    evt = seaflowfile.date_evt_files(in_files, sfl_df)
    print(f"Found {len(in_files)} EVT files", file=sys.stderr)
    if sfl_path:
        print(f"Matched {len(evt)} EVT files to SFL entries", file=sys.stderr)
    parquet_files = []
    for row in evt.itertuples(index=False):
        if not pathlib.Path(str(row.path)).exists():
            raise FileNotFoundError(f"No such file or directory: '{row.path}'")
        sf = seaflowfile.SeaFlowFile(row.path, date=row.date)
        parquet_files.append(str(out_dir / sf.path_dayofyear / f"{sf.filename_orig}.parquet"))
    evt["parquet"] = parquet_files

    with pd.option_context("display.max_colwidth", 200):
        print(evt.head(), file=sys.stderr)
        print("...")
        print(evt.tail(), file=sys.stderr)

    parallel = Parallel(n_jobs=n_jobs, verbose=1 if progress else 0)
    args = zip(evt["path"].to_list(), evt["parquet"].to_list())
    results = parallel(delayed(_binary_to_parquet)(*a) for a in args)
    error_lines = [f"  {r[0]}: {r[1]}" for r in results if r and (r[1] is not None)]
    if error_lines:
        print('Errors:', file=sys.stderr)
        print(
            '\n'.join(error_lines),
            file=sys.stderr
        )
    ok = len([r for r in results if r and (r[1] is None)])
    print('Converted %d / %d files' % (ok, len(in_files)), file=sys.stderr)


def _binary_to_parquet(infile, outfile):
    try:
        fileio.binary_to_parquet(infile, outfile, empty_output=True)
    except (errors.FileError, IOError) as e:
        return (infile, e)
    return (infile, None)


@evt_cmd.command('compare')
@click.option('-a', '--all', 'report_all', is_flag=True,
    help='Show information for all files. If not specified then only file that differ are printed.')
@click.option('-n', '--n-jobs', default=1, type=int, help='worker jobs')
@click.option('-p', '--progress', is_flag=True,
    help='Print progress')
@click.option('-r', '--reduced-columns', is_flag=True,
    help=f'Hash on the reduced column set {particleops.REDUCED_COLUMNS}')
@click.argument('paths', nargs=2, type=click.Path(exists=True, path_type=pathlib.Path))
def compare_evt_cmd(report_all, n_jobs, progress, reduced_columns, paths):
    """
    Compare EVT files for equality.

    paths can either be two EVT files or two directories of EVT files. If
    arguments are directories, EVT files are matched by canonical SeaFlow file
    ID, and files with unparsable names are ignored. If both arguments are files
    then file name parsing is not performed.

    Equality is determined by MD5 of df.to_numpy().tobytes(). A TSV table of
    results is printed to STDOUT for EVT dataframes that differ. If --all,
    all files are reported, even those that match. Read and parsing errors are
    printed to STDERR.
    """
    if paths[0].is_file() and paths[1].is_file():
        # Ignore IDs to allow comparison between files with arbitrary names
        x_files = pd.DataFrame({"id": ["-"], "path": [str(paths[0])]})
        y_files = pd.DataFrame({"id": ["-"], "path": [str(paths[1])]})
    elif paths[0].is_dir() and paths[1].is_dir():
        x_ids, x_paths = [], []
        y_ids, y_paths = [], []
        x_file_paths = util.expand_file_list([str(paths[0])])
        x_file_paths = seaflowfile.keep_evt_files(x_file_paths)
        y_file_paths = util.expand_file_list([str(paths[1])])
        y_file_paths = seaflowfile.keep_evt_files(y_file_paths)
        for f in x_file_paths:
            try:
                sf = seaflowfile.SeaFlowFile(f)
            except errors.FileError as e:
                print('error parsing file name for %s: %s' % (f, str(e)))
            else:
                x_ids.append(sf.file_id)
                x_paths.append(f)
        
        for f in y_file_paths:
            try:
                sf = seaflowfile.SeaFlowFile(f)
            except errors.FileError as e:
                print('error parsing file name for %s: %s' % (f, str(e)))
            else:
                y_ids.append(sf.file_id)
                y_paths.append(f)
        x_files = pd.DataFrame({"id": x_ids, "path": x_paths})
        y_files = pd.DataFrame({"id": y_ids, "path": y_paths})
    else:
        raise click.ClickException('path arguments must be two files or two directories')

    if reduced_columns:
        cols = particleops.REDUCED_COLUMNS
    else:
        cols = None

    m = x_files.merge(y_files, how='outer', on='id', indicator=True)
    if m['id'].duplicated().any():
        print('the following File IDs were duplicated in at least one dir and will be ignored', file=sys.stderr)
        m.loc[m['id'].duplicated(), ['id', 'path_x', 'path_y']].to_string(index=False, buf=sys.stderr)
        m = m.loc[~m['id'].duplicated()]

    if progress:
        verbose = 1
    else:
        verbose = 0
    parallel = Parallel(n_jobs=n_jobs, verbose=verbose)
    work_func = partial(_compare_two_files, cols=cols)
    m_hashed = pd.DataFrame(list(parallel(delayed(work_func)(r[1]) for r in m.iterrows())))
    m_hashed.sort_values(by='id', key=_idkey, inplace=True)

    if report_all:
        m_output = m_hashed
    else:
        m_output = m_hashed.loc[~m_hashed['equal']]
    if len(m_output):
        m_output.to_string(index=False, buf=sys.stdout)
    print()


def _compare_two_files(s, cols=None):
    s = s.copy()

    if pd.isna(s["path_x"]):
        x = pd.Series({ "version": "-", "err": None, "hash": "-", "count": 0 })
    else:
        x = fileio.validate_evt_file(s["path_x"], checksum=True, cols=cols)
    if pd.isna(s["path_y"]):
        y = pd.Series({ "version": "-", "err": None, "hash": "-", "count": 0 })
    else:
        y = fileio.validate_evt_file(s["path_y"], checksum=True, cols=cols)

    s["equal"] = (x["hash"] == y["hash"]) and (x["hash"] != "-") and (y["hash"] != "-")
    s["hash_x"] = x["hash"]
    s["hash_y"] = y["hash"]
    s["count_x"] = x["count"]
    s["count_y"] = y["count"]
    s["version_x"] = x["version"]
    s["version_y"] = y["version"]
    s["err_x"] = x["err"]
    s["err_y"] = y["err"]

    return s
