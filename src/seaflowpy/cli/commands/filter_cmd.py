from builtins import range
from builtins import zip
import datetime
import json
import os
import random
import sys
import time
import urllib
import botocore
import click
import pandas as pd
import pkg_resources
from fabric.api import (cd, env, execute, hide, local, parallel, put, puts,
    quiet, run, settings, show, sudo, task)
from fabric.network import disconnect_all
from seaflowpy import clouds
from seaflowpy import conf
from seaflowpy import db
from seaflowpy import errors
from seaflowpy import filterevt
from seaflowpy import util
from seaflowpy import seaflowfile


@click.group()
def filter_cmd():
    """EVT filtering subcommand."""
    pass


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


@filter_cmd.command('local')
@click.option('-D', '--delta', is_flag=True,
    help='Filter EVT files which are not already present in the opp table.')
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
@util.quiet_keyboardinterrupt
def local_filter_evt_cmd(delta, evt_dir, s3_flag, dbpath, limit, opp_dir, process_count, resolution):
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
        _filter_params = db.get_latest_filter(dbpath)
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))

    # Capture run parameters and information
    v = {
        'delta': delta,
        'evt_dir': evt_dir,
        's3': s3_flag,
        'limit': limit,
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
    if evt_dir:
        evt_files = seaflowfile.sorted_files(seaflowfile.find_evt_files(evt_dir))
    elif s3_flag:
        # Make sure configuration for s3 is ready to go
        config = conf.get_aws_config(s3_only=True)
        cloud = clouds.AWS(config.items("aws"))
        # Make sure try to access S3 up front to setup AWS credentials before
        # launching child processes.
        try:
            evt_files = cloud.get_files(cruise)
            evt_files = seaflowfile.sorted_files(seaflowfile.keep_evt_files(evt_files))  # Only keep EVT files
        except botocore.exceptions.NoCredentialsError as e:
            print('Please configure aws first:', file=sys.stderr)
            print('  $ conda install aws', file=sys.stderr)
            print('  or', file=sys.stderr)
            print('  $ pip install aws', file=sys.stderr)
            print('  then', file=sys.stderr)
            print('  $ aws configure', file=sys.stderr)
            raise click.Abort()

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

    # Filter
    try:
        filterevt.filter_evt_files(
            files_df,
            dbpath,
            opp_dir,
            s3=s3_flag,
            worker_count=process_count,
            every=resolution
        )
    except errors.SeaFlowpyError as e:
        raise click.ClickException(str(e))


# ---------------------------------------------------------------------------- #
# Remote filter command section
# ---------------------------------------------------------------------------- #

REMOTE_WORK_DIR = "/mnt/ramdisk"
REMOTE_DB_DIR = "{}/dbs".format(REMOTE_WORK_DIR)
LATEST_EXE_URL = "https://github.com/armbrustlab/seaflowpy/releases/latest/download/seaflowpy-linux64"

def validate_executable_file(ctx, param, value):
    if value and len(value):
        if not os.path.isfile(value):
            raise click.BadParameter(f'{value} does not exist or is not a file')
    return value

def validate_positive_int(ctx, param, value):
    if value <= 0:
        raise click.BadParameter('%s must be > 0' % param)
    return value

@filter_cmd.command('remote')
@click.option('-D', '--dryrun', is_flag=True,
    help='Show cruise to host assignments without starting instances.')
@click.option('-e', '--executable', metavar='EXE', callback=validate_executable_file,
    help='Seaflowpy single-file executable to run on the remote linux machine. [default: github latest release]')
@click.option('-i', '--instance-count', default=1, show_default=True, metavar='N', callback=validate_positive_int,
    help='Number of cloud instances to use.')
@click.option('-n', '--no-cleanup', is_flag=True, default=False, show_default=True,
    help='Don\'t cleanup resources.')
@click.option('-o', '--output-dir', metavar='DIR', required=True,
    help='Output directory for SQLite3 database and OPP binary files for each cruise. Will be created if does not exist')
@click.option('-p', '--process-count', default=96, show_default=True, metavar='N', callback=validate_positive_int,
    help='Number of processes to use in filtering.')
@click.option('-r', '--ramdisk-size', default=60, show_default=True, metavar='GiB', callback=validate_positive_int,
    help='Size of ramdisk in GiB, limited by instance RAM.')
@click.option('-t', '--instance-type', default='c5.24xlarge', show_default=True, metavar='EC2_TYPE',
    help='EC2 instance type to use. Change with caution. The instance type must have be able to attach 2 instance store devices.')
@click.argument('dbs', nargs=-1, type=click.Path(exists=True))
def remote_filter_evt_cmd(dryrun, executable, instance_count, no_cleanup,
                          output_dir, process_count, ramdisk_size,
                          instance_type, dbs):
    """Filter EVT data on remote servers.

    SQLite3 db files must contain filter parameters and cruise name
    """
    print("Started at {}{}".format(datetime.datetime.utcnow().isoformat(), os.linesep))

    # Print defined parameters and information
    v = {
        'dbs': dbs,
        'executable': executable,
        'output_dir': output_dir,
        'dryrun': dryrun,
        'instance_count': instance_count,
        'no_cleanup': no_cleanup,
        'process_count': process_count,
        'instance_type': instance_type,
        'ramdisk_size': ramdisk_size,
        'version': pkg_resources.get_distribution("seaflowpy").version
    }
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    print('Run parameters and information:')
    print(json.dumps(v, indent=2))
    print('')

    # Make sure configuration for aws and ssh is ready to go
    config = conf.get_aws_config()
    conf.get_ssh_config(config)
    cloud = clouds.AWS(config.items('aws'))

    # If local executable is not given download latest from github
    remove_executable = False
    if not executable:
        remove_executable = True  # mark this file for deletion at exit
        executable = download_latest_linux()

    # Configure fabric
    env.connection_attempts = 6
    # Tell fabric the SSH user name and key file location
    env.user = config.get('ssh', 'ssh-user')
    env.key_filename = os.path.expanduser(config.get('ssh', 'ssh-private-key-file'))

    try:
        if len(dbs) > 0:
            print('Getting lists of files for each cruise')
            cruise_files = {}
            for dbfile in dbs:
                # Make sure file exists
                if not os.path.exists(dbfile):
                    raise click.ClickException('DB file {} does not exist'.format(dbfile))
                # Make sure db has filter parameters filled in
                try:
                    filter_table = db.get_latest_filter(dbfile)
                except errors.SeaFlowpyError as e:
                    raise click.ClickException('No filter parameters found in database file {}'.format(dbfile))
                if len(filter_table) != 3:
                    raise click.ClickException('Unusual filter parameters found in database file {}'.format(dbfile))
                # Get cruise name DB
                try:
                    c = db.get_cruise(dbfile)
                except errors.SeaFlowpyError as e:
                    raise click.ClickException('Could not retrieve cruise name from DB. {}'.format(e))
                try:
                    evt_files = seaflowfile.sorted_files(seaflowfile.keep_evt_files(cloud.get_files(c)))
                except botocore.exceptions.NoCredentialsError as e:
                    print('Please configure aws first:', file=sys.stderr)
                    print('  $ pip install awscli', file=sys.stderr)
                    print('  then', file=sys.stderr)
                    print('  $ aws configure', file=sys.stderr)
                    raise click.Abort()

                # Check for duplicates, exit with message if any exist
                uniques = {seaflowfile.SeaFlowFile(f).file_id for f in evt_files}
                if len(uniques) < len(evt_files):
                    raise click.ClickException('Duplicate EVT file(s) detected')

                # Filter cruise files by SFL entries
                try:
                    sfl_df = db.get_sfl_table(dbfile)
                except errors.SeaFlowpyError as e:
                    print('Error retrieving SFL file list from DB: {}'.format(e))
                    return 1
                sfl_files = sfl_df["file"].tolist()
                # Find intersection of SFL files and EVT files
                cruise_files[c] = seaflowfile.filtered_file_list(evt_files, sfl_files)
                print('{:<20} sfl={} evt={} intersection={}'.format(
                    c, len(sfl_files), len(evt_files), len(cruise_files[c])
                ))
            print('')

            if dryrun:
                # Create dummy host list
                print('Creating {} dummy hosts'.format(instance_count))
                env.hosts = ['dummy{}'.format(i) for i in range(instance_count)]
            else:
                print('Starting {} instances'.format(instance_count))
                result = cloud.start(
                    count=instance_count,
                    instance_type=instance_type
                )
                for iid, iip in zip(result['InstanceIds'], result['publicips']):
                    print('  InstanceId = {}, IP = {}'.format(iid, iip))
                env.hosts.extend(result['publicips'])
            print('')

            # Fairly divide cruises into hosts based on number of files
            print('Assigning cruises to {} hosts'.format(len(env.hosts)))
            host_assignments = assign_keys_to_hosts(env.hosts, cruise_files)
            for h in host_assignments:
                htotal = sum([c[1] for c in host_assignments[h]])
                print('{:<20} {}'.format(h, htotal))
                for c in host_assignments[h]:
                    print('  {:<18} {}'.format(c[0], c[1]))
            print('')

            if dryrun:
                print('Dry run complete')
                print('')
                return 0

            print('Waiting for hosts to come up with SSH')
            execute(wait_for_up)

            print('Creating initial ramdisk')
            with hide('output'):
                execute(create_ramdisk, ramdisk_size)

            print('Transfer AWS credentials')
            with hide('output'):
                execute(rsync_put, ['~/.aws/'], '.aws')

            print('Transfer seaflowpy configuration')
            with hide('output'):
                execute(rsync_put, ['~/.seaflowpy/'], '.seaflowpy')

            print('Transfer initial databases')
            execute(mkdir, REMOTE_DB_DIR)  # create db dir on each host
            with hide('output'):
                execute(rsync_put, dbs, REMOTE_DB_DIR)

            print('Install system dependencies')
            execute(install_system_dependencies)

            print('Upload seaflowpy executable')
            execute(upload_seaflowpy, executable)

            # Host list in env.hosts should be populated now and all machines up
            print('Filter data')
            execute(filter_cruise, host_assignments, output_dir,
                    process_count)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        disconnect_all()  # always disconnect SSH connections
        if not no_cleanup:
            cloud.cleanup()  # clean up in case of any unhandled exceptions
        # Clean up seaflowpy executable we downloaded
        if remove_executable:
            try:
                os.remove(executable)
            except OSError as e:
                print('Error: could not delete temporary seaflowpy executable: {} - {}'.format(executable, e.strerror), file=sys.stderr)

        print('Finished at {}'.format(datetime.datetime.utcnow().isoformat()))

    return 0


def check_db_filter_params(dbfile):
    filter_table = db.get_latest_filter(dbfile)
    return len(filter_table) == 3


def count_things(data):
    counts = [(k, len(v)) for k, v in list(data.items())]
    counts.sort(key=lambda x: x[1], reverse=True)
    return counts


def assign_keys_to_hosts(hosts, data):
    assignments = {}
    for h in hosts:
        assignments[h] = []

    counts = count_things(data)
    for k, n in counts:
        fewest = {'host': None, 'count': None}
        for h in hosts:
            # What is count of things assigned to this host plus this new item?
            total = sum([x[1] for x in assignments[h]]) + n
            # If this total has the fewest items so far, record this host
            # assignment
            if fewest['host'] is None or total < fewest['count']:
                fewest['host'] = h
                fewest['count'] = total
        assignments[fewest['host']].append((k, n))

    return assignments


@task
@parallel
def wait_for_up():
    # Try to run hostname to establish host is up and SSH is running
    with hide('everything'):
        # don't wait longer than 10 minutes
        run('hostname', timeout=600)


@task
@parallel
def create_ramdisk(gigabytes):
    # Make ramdisk at REMOTE_WORK_DIR
    with hide('everything'):
        # don't wait longer than 10 minutes
        sudo('mkdir -p {}'.format(REMOTE_WORK_DIR), timeout=600)
        sudo('mount -t tmpfs -o size={}G tmpfs {}'.format(gigabytes, REMOTE_WORK_DIR), timeout=600)


@task
@parallel
def rsync_put(localpaths, remotepath):
    # Delete to remote
    rsynccmd = [
        'rsync', '-au', '--stats', '--delete', '-e',
        "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename)
    ]
    rsynccmd.extend(localpaths)
    rsynccmd.append('{}@{}:{}'.format(env.user, env.host_string, remotepath))
    result = local(' '.join(rsynccmd), capture=True)
    return result


@task
@parallel
def rsync_get(remotepath, localpath):
    # no delete to local
    rsynccmd = [
        'rsync', '-au', '--stats', '-e',
        "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename),
        '{}@{}:{}'.format(env.user, env.host_string, remotepath),
        localpath
    ]
    result = local(' '.join(rsynccmd), capture=True)
    return result

@task
@parallel
def mkdir(d):
    with quiet():
        if run('test -d {}'.format(d)).failed:
            run('mkdir -p {}'.format(d))

@task
@parallel
def install_system_dependencies():
    with quiet():
        sudo('apt-get update -q')
        sudo('apt-get install -qy zip')

@task
@parallel
def upload_seaflowpy(executable):
    put(executable, '/usr/local/bin/seaflowpy', use_sudo=True)
    sudo('chmod +x /usr/local/bin/seaflowpy')
    with show('stdout'):
        run('seaflowpy version')

@task
@parallel
def filter_cruise(host_assignments, output_dir, process_count=16):
    util.mkdir_p(output_dir)

    cruises = [x[0] for x in host_assignments[env.host_string]]
    cruise_results = {}
    with cd(REMOTE_WORK_DIR):
        for c in cruises:
            puts('Filtering cruise {}'.format(c))
            with hide('commands'):
                run('mkdir {}'.format(c))
            with hide('commands'):
                run('cp {}/{}.db {}'.format(REMOTE_DB_DIR, c, c))
            with cd(c):
                text = {
                    'cruise': c,
                    'process_count': process_count
                }
                with settings(warn_only=True), hide('output'):
                    result = run(
                        'seaflowpy filter local --s3 -d {cruise}.db -p {process_count} -o {cruise}_opp'.format(**text),
                        timeout=10800
                    )
                    cruise_results[c] = result

            puts(result)

            if result.succeeded:
                puts('Filtering successfully completed for cruise {}'.format(c))
                puts('Zipping cruise {} results into single file archive.'.format(c))
                with settings(warn_only=True), hide('output'):
                    # Dont' compress, assuming all OPP data is already
                    # gzipped. This zip file is just a more conveniently
                    # indexed and cross-platform tar archive.
                    result = run('zip -r -0 -q {}.zip {}'.format(c, c), timeout=10800)

                puts('Returning results for cruise {}'.format(c))
                rsyncout = execute(
                    # rsync files in cruise results dir to local cruise dir
                    rsync_get,
                    os.path.join(REMOTE_WORK_DIR, '{}.zip'.format(c)),
                    output_dir + '/',
                    hosts=[env.host_string]
                )

                # Print rsync output on source host, even though this is run
                # on local, just to make it clear in logs which host is being
                # transferred from
                puts(rsyncout[env.host_string])

                # Erase data for this cruise on remote filtering server
                puts('Removing results for cruise {} after successful transfer'.format(c))
                with hide('commands'):
                    run('rm -rf {} {}.zip'.format(c, c))
            else:
                sys.stderr.write('Filtering failed for cruise {}\n'.format(c))

            # Always write log output
            logpath = os.path.join(output_dir, '{}.seaflowpy_filter.log'.format(c))

            with open(logpath, 'w') as logfh:
                logfh.write('command={}\n'.format(cruise_results[c].command))
                logfh.write('real_command={}\n'.format(cruise_results[c].real_command))
                logfh.write(norm(cruise_results[c].stdout) + '\n')

    return cruise_results

# Fabric3 seems to be defaulting to /r/n line-endings. This function should fix
# that.
def norm(text):
    """Normalize line-endings in a text string."""
    return text.replace('\r\n', os.linesep).replace('\r', os.linesep)

def download_latest_linux():
    """Download latest linux executable and return file path"""
    retry_limit = 3
    retries = 0
    while True:
        try:
            filename, _ = urllib.request.urlretrieve(LATEST_EXE_URL)
        except (urllib.error.ContentTooShortError, urllib.error.URLError, urllib.error.HTTPError):
            if retries == retry_limit:
                raise click.ClickException("couldn't download linux exe")
            retries += 1
            time.sleep(2**retries + random.random())
        else:
            break
    return filename
