import json
import logging

import click
import pandas as pd
import pkg_resources
from seaflowpy import db
from seaflowpy import errors
from seaflowpy import filterevt
from seaflowpy import seaflowfile

logger = logging.getLogger(__name__.split(".")[0])
logger.setLevel(logging.NOTSET)
logging_ch = logging.StreamHandler()
logging_ch.setFormatter(
    logging.Formatter(fmt="%(asctime)s pid=%(process)-10s %(levelname)s %(module)s %(message)s")
)
logger.addHandler(logging_ch)

def validate_limit(ctx, param, value):
    if value is not None and value < 1:
        raise click.BadParameter('if limit is set, it must be >= 1')
    return value


def validate_process_count(ctx, param, value):
    if value < 1:
        raise click.BadParameter('process_count must be >= 1')
    return value


def validate_resolution(ctx, param, value):
    if value <= 0 or value > 100:
        raise click.BadParameter('resolution must be a number between 1 and 100 inclusive.')
    return value


@click.command()
@click.option('-D', '--delta', is_flag=True,
    help='Filter EVT files which are not already present in the opp table.')
@click.option('-e', '--evt-dir', metavar='DIR', type=click.Path(exists=True), required=True,
    help='EVT directory path (required unless --s3)')
@click.option('-d', '--db', 'dbpath', required=True, metavar='FILE', type=click.Path(exists=True),
    help='Popcycle SQLite3 db file with filter parameters and cruise name.')
@click.option('-l', '--limit', type=int, metavar='N', callback=validate_limit,
    help='Limit number of files to process.')
@click.option('-m', '--max-particles-per-file', type=int, default=filterevt.max_particles_per_file_default,
    show_default=True, metavar='N', callback=validate_limit,
    help='Only filter files with an event count <= this limit.')
@click.option('-o', '--opp-dir', metavar='DIR',
    help='Directory in which to save OPP files. Will be created if does not exist.')
@click.option('-p', '--process-count', default=1, show_default=True, metavar='N', callback=validate_process_count,
    help='Number of processes to use in filtering.')
@click.option('-r', '--resolution', default=10.0, show_default=True, metavar='N', callback=validate_resolution,
    help='Progress update resolution by %%.')
def filter_cmd(delta, evt_dir, dbpath, limit, max_particles_per_file, opp_dir, process_count, resolution):
    """Filter EVT data locally."""
    # Find cruise in db
    try:
        cruise = db.get_cruise(dbpath)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))

    # Find filter parameters in db. Won't use them yet but better to check
    # upfront
    try:
        _filter_params = db.get_latest_filter(dbpath)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))

    # Capture run parameters and information
    v = {
        'delta': delta,
        'evt_dir': evt_dir,
        'limit': limit,
        'max_particles_per_file': max_particles_per_file,
        'db': dbpath,
        'opp_dir': opp_dir,
        'process_count': process_count,
        'resolution': resolution,
        'version': pkg_resources.get_distribution("seaflowpy").version,
        'cruise': cruise
    }
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters

    # Print run parameters
    print('Run parameters and information:')
    print(json.dumps(v, indent=2))
    print('')

    # Find EVT files
    print('Getting lists of files to filter')
    evt_files = seaflowfile.sorted_files(seaflowfile.find_evt_files(evt_dir))

    # Check for duplicates, exit with message if any exist
    # This could be caused by gzipped and uncompressed files in the same location
    uniques = {seaflowfile.SeaFlowFile(f).file_id for f in evt_files}
    if len(uniques) < len(evt_files):
        raise click.ClickException('Duplicate EVT file(s) detected')

    # Get DataFrame of file IDs, paths, dates in common between discovered
    # EVT file paths and SFL files.
    try:
        sfl_df = db.get_sfl_table(dbpath)
    except (errors.SeaFlowpyError, KeyError, ValueError) as e:
        raise click.ClickException(str(e))
    files_df = seaflowfile.date_evt_files(evt_files, sfl_df)

    # Find intersection of SFL files and EVT files
    print('sfl={} evt={} intersection={}'.format(len(sfl_df), len(evt_files), len(files_df)))

    if delta:
        # Limit files to those not present in the opp table
        try:
            opp_df = db.get_opp_table(dbpath)
            opp_df = opp_df.rename(columns={"file": "file_id"}).loc[:, "file_id"]
        except (errors.SeaFlowpyError, KeyError, ValueError) as e:
            raise click.ClickException(str(e))
        # Find files in common
        merged = pd.merge(files_df, opp_df, how='left', on=["file_id"], indicator=True)
        evt_n = len(files_df)
        files_df = merged[merged["_merge"] == "left_only"]
        print('opp={} evt={} evt_not_in_opp={}'.format(len(opp_df), evt_n, len(files_df)))

    # Restrict length of file list with --limit
    if (limit is not None) and (limit > 0):
        files_df = files_df.head(limit)

    if len(files_df.index) > 0:
        # Filter
        try:
            filterevt.filter_evt_files(
                files_df,
                dbpath,
                opp_dir,
                worker_count=process_count,
                every=resolution,
                max_particles_per_file=max_particles_per_file
            )
        except errors.SeaFlowpyError as e:
            raise click.ClickException(str(e))
