from __future__ import absolute_import, print_function
import botocore
import click
import json
import pkg_resources
import sys
from seaflowpy import clouds
from seaflowpy import conf
from seaflowpy import db
from seaflowpy import errors
from seaflowpy import evt
from seaflowpy import filterevt


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
@click.option('-e', '--evt-dir', metavar='DIR', type=click.Path(exists=True),
    help='EVT directory path (required unless --s3)')
@click.option('-s', '--s3', 's3_flag', is_flag=True,
    help='Read EVT files from s3://S3_BUCKET/CRUISE where CRUISE is detected in the sqlite db metadata table (required unless --evt_dir).')
@click.option('-d', '--db', 'dbpath', required=True, metavar='FILE', type=click.Path(exists=True),
    help='Popcycle SQLite3 db file with filter parameters and cruise name.')
@click.option('-l', '--limit', type=int, metavar='N', callback=validate_limit,
    help='Limit number of files to process.')
@click.option('-o', '--opp-dir', metavar='DIR',
    help='Directory in which to save OPP files. Will be created if does not exist.')
@click.option('-p', '--process-count', default=1, show_default=True, metavar="N", callback=validate_process_count,
    help='Number of processes to use in filtering.')
@click.option('-r', '--resolution', default=10.0, show_default=True, metavar='N', callback=validate_resolution,
    help='Progress update resolution by %%.')
def filter_evt_cmd(evt_dir, s3_flag, dbpath, limit, opp_dir, process_count, resolution):
    """Filter EVT data locally."""
    # Validate args
    if not evt_dir and not s3_flag:
        raise click.UsageError('One of --evt_dir or --s3 must be provided')

    # Find cruise in db
    try:
        cruise = db.get_cruise(dbpath)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))

    # Find filter parameters in db. Won't use them yet but better to check
    # upfront
    try:
        filter_params = db.get_latest_filter(dbpath)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))

    # Capture software version
    version = pkg_resources.get_distribution("seaflowpy").version

    # Capture run parameters and information
    v = {
        'evt_dir': evt_dir,
        's3': s3_flag,
        'limit': limit,
        'db': dbpath,
        'opp_dir': opp_dir,
        'process_count': process_count,
        'resolution': resolution,
        'version': version,
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
    if evt_dir:
        files = evt.find_evt_files(evt_dir)
    elif s3_flag:
        # Make sure configuration for s3 is ready to go
        config = conf.get_aws_config(s3_only=True)
        cloud = clouds.AWS(config.items("aws"))
        # Make sure try to access S3 up front to setup AWS credentials before
        # launching child processes.
        try:
            files = cloud.get_files(cruise)
            files = evt.parse_file_list(files)  # Only keep EVT files
        except botocore.exceptions.NoCredentialsError as e:
            print("Please configure aws first:")
            print("  $ conda install aws")
            print("  or")
            print("  $ pip install aws")
            print("  then")
            print("  $ aws configure")
            raise click.Abort()

    # Restrict length of file list with --limit
    if (limit is not None) and (limit > 0):
        files = files[:limit]

    # Filter
    try:
        filterevt.filter_evt_files(files, dbpath, opp_dir, s3=s3_flag,
                                   worker_count=process_count,
                                   every=resolution)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))
