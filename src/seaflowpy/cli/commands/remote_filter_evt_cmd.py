import argparse
import botocore
import click
import datetime
import json
import os
import pkg_resources
import sys
from builtins import range
from builtins import zip
from fabric.api import (cd, env, execute, get, hide, local, parallel, puts,
    quiet, run, settings, show, task)
from fabric.network import disconnect_all
from seaflowpy import clouds
from seaflowpy import conf
from seaflowpy import db
from seaflowpy import errors
from seaflowpy import util

REMOTE_SOURCE_DIR = '/home/ubuntu/src/seaflowpy'
REMOTE_WORK_DIR = "/mnt/ramdisk"
REMOTE_DB_DIR = "{}/dbs".format(REMOTE_WORK_DIR)


def validate_positive_int(ctx, param, value):
    if value <= 0:
        raise click.BadParameter('%s must be > 0' % param)
    return value

def validate_source_dir(ctx, param, value):
    if value and len(value):
        if not value.endswith('/'):
            value = value + '/'
        if not os.path.isdir(value):
            raise click.BadParameter(f'{source_dir} does not exist or is not a directory')
    return value

@click.command()
@click.option('-b', '--branch', metavar='NAME', default='master', show_default=True,
    help='Which branch of seaflowpy github repo to use on remote servers.')
@click.option('-D', '--dryrun', is_flag=True,
    help='Show cruise to host assignments without starting instances.')
@click.option('-i', '--instance-count', default=1, show_default=True, metavar='N', callback=validate_positive_int,
    help='Number of cloud instances to use.')
@click.option('-n', '--no-cleanup', is_flag=True, default=False, show_default=True,
    help='Don\'t cleanup resources.')
@click.option('-o', '--output-dir', metavar='DIR', required=True,
    help='Output directory for SQLite3 database and OPP binary files for each cruise. Will be created if does not exist')
@click.option('-p', '--process-count', default=16, show_default=True, metavar='N', callback=validate_positive_int,
    help='Number of processes to use in filtering.')
@click.option('-r', '--ramdisk-size', default=60, show_default=True, metavar='GiB', callback=validate_positive_int,
    help='Size of ramdisk in GiB, limited by instance RAM.')
@click.option('-s', '--source-dir', metavar='DIR', callback=validate_source_dir,
    help='Local seaflowpy source directory to use on remote servers. This overrides source code pulls from github.')
@click.option('-t', '--instance-type', default='c5.9xlarge', show_default=True, metavar='EC2_TYPE',
    help='EC2 instance type to use. Change with caution. The instance type must have be able to attach 2 instance store devices.')
@click.argument('dbs', nargs=-1, type=click.Path(exists=True))
def remote_filter_evt_cmd(branch, dryrun, instance_count, no_cleanup,
                          output_dir, process_count, ramdisk_size, source_dir,
                          instance_type, dbs):
    """Filter EVT data on remote servers.

    SQLite3 db files must contain filter parameters and cruise name
    """
    print("Started at {}".format(datetime.datetime.utcnow().isoformat()))

    # Print defined parameters and information
    v = {
        'branch': branch,
        'dbs': dbs,
        'output_dir': output_dir,
        'dryrun': dryrun,
        'instance_count': instance_count,
        'no_cleanup': no_cleanup,
        'process_count': process_count,
        'instance_type': instance_type,
        'source_dir': source_dir,
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

    # Configure fabric
    env.connection_attempts = 6
    # Tell fabric the SSH user name and key file location
    env.user = config.get('ssh', 'user')
    env.key_filename = os.path.expanduser(config.get('ssh', 'ssh-key-file'))

    try:
        print('Getting lists of files for each cruise')
        cruise_files = {}
        for dbfile in dbs:
            # Make sure file exists
            if not os.path.exists(dbfile):
                print('DB file {} does not exist'.format(dbfile))
                return 1
            # Make sure db has filter parameters filled in
            try:
                filter_table = db.get_latest_filter(dbfile)
            except errors.SeaFlowpyError as e:
                print('No filter parameters found in database file {}'.format(dbfile))
                return 1
            if len(filter_table) != 3:
                print('Unusual filter parameters found in database file {}'.format(dbfile))
                return 1
            # Get cruise name DB
            try:
                c = db.get_cruise(dbfile)
            except errors.SeaFlowpyError as e:
                print('Error retrieving cruise name from DB: {}'.format(e))
                return 1
            try:
                cruise_files[c] = cloud.get_files(c)
            except botocore.exceptions.NoCredentialsError as e:
                print('Please configure aws first:')
                print('  $ conda install aws')
                print('  or')
                print('  $ pip install aws')
                print('  then')
                print('  $ aws configure')
                return 1
            print('{:<20} {}'.format(c, len(cruise_files[c])))
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
            return

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

        print('Install miniconda 3')
        execute(install_miniconda3)

        execute(mkdir, REMOTE_SOURCE_DIR)  # create source dir on each host
        if source_dir:
            print('Transfer local seaflowpy source code')
            execute(rsync_put, [source_dir], REMOTE_SOURCE_DIR)
        else:
            print('Pull seaflowpy source code')
            execute(pull_seaflowpy, branch, REMOTE_SOURCE_DIR)

        print('Install seaflowpy')
        execute(install_seaflowpy)

        # Host list in env.hosts should be populated now and all machines up
        print('Filter data')
        execute(filter_cruise, host_assignments, output_dir,
                process_count)
    finally:
        disconnect_all()  # always disconnect SSH connections
        if not no_cleanup:
            cloud.cleanup()  # clean up in case of any unhandled exceptions
        print('Finished at {}'.format(datetime.datetime.utcnow().isoformat()))


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
        run('sudo mkdir -p {}'.format(REMOTE_WORK_DIR), timeout=600)
        run('sudo mount -t tmpfs -o size={}G tmpfs {}'.format(gigabytes, REMOTE_WORK_DIR), timeout=600)


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
def install_miniconda3():
    install_url = 'https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh'
    install_script = '$HOME/miniconda3.sh'
    install_dir = '$HOME/miniconda3'
    with hide('stdout'):
        run(f'wget {install_url} -O {install_script}')
        run(f'sh {install_script} -b -p {install_dir}')
        run(f'echo \'export PATH="{install_dir}/bin:$PATH"\' >>$HOME/.bash_profile')
        run(f'export PATH="{install_dir}/bin:$PATH"')
        with show('stdout'):
            run('which python')
            run('python -V')
            run('echo $PATH')

@task
@parallel
def pull_seaflowpy(branch, source_dir):
    with quiet():
        with show('running', 'warnings', 'stderr'):
            run('git clone https://github.com/armbrustlab/seaflowpy {}'.format(REMOTE_SOURCE_DIR))
    with cd(REMOTE_SOURCE_DIR), hide('stdout'):
        run(f'git checkout {branch}')

@task
@parallel
def install_seaflowpy():
    with cd(REMOTE_SOURCE_DIR), hide('stdout'):
        # If this is a git repo, clean it first.
        run('git rev-parse --is-inside-work-tree >/dev/null 2>&1 && git clean -fdx')
        run('conda env create -n seaflowpy -f environment.lock.yml')
        run('conda activate seaflowpy')
        run('pip install .')
        run('pytest')
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
                        'seaflowpy filter --s3 -d {cruise}.db -p {process_count} -o {cruise}_opp'.format(**text),
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
            else:
                sys.stderr.write('Filtering failed for cruise {}\n'.format(c))

            # Capture conda environment information
            with settings(warn_only=True), hide('output'):
                result = run('conda list')
            if result.succeeded:
                conda_env_text = result
            else:
                conda_env_text = ""

            # Always write log output
            logpath = os.path.join(output_dir, '{}.seaflowpy_filter.log'.format(c))

            with open(logpath, 'w') as logfh:
                logfh.write('command={}\n'.format(cruise_results[c].command))
                logfh.write('real_command={}\n'.format(cruise_results[c].real_command))
                logfh.write(cruise_results[c] + '\n')
                logfh.write(cruise_results[c] + '\n')
                logfh.write('conda env list' + '\n')
                logfh.write(conda_env_text + '\n')

    return cruise_results
