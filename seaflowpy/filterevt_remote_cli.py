#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
from builtins import zip
from builtins import range
import argparse
import botocore
from . import clouds
from . import conf
from . import db
import datetime
import json
import os
import pkg_resources
import sys
from . import util
from fabric.api import (cd, env, execute, get, hide, local, parallel, puts,
    quiet, run, settings, show, task)
from fabric.network import disconnect_all


REMOTE_WORK_DIR = "/mnt/ramdisk"
REMOTE_DB_DIR = "{}/dbs".format(REMOTE_WORK_DIR)


def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A program to filter EVT data on remote servers (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-d", "--dbs", nargs="+", required=True, metavar="FILE",
                   help="""Popcycle sqlite3 databases with filter parameters.
                   Database filenames should match cruise names in S3. e.g.
                   SCOPE_1.db matches cruise 'SCOPE_1'. (required)""")

    p.add_argument("-o", "--output_dir", metavar="DIR", required=True,
                   help="""Directory in which to save SQLite3 database and
                   OPP binary files for each cruise. Will be created if does
                   not exist. (required)""")

    p.add_argument("-p", "--process_count", type=int, default=16, metavar="N",
                   help="""Number of processes to use in filtering.""")
    p.add_argument("-i", "--instance_count", type=int, default=1, metavar="N",
                   help="""Number of cloud instances to use.""")
    p.add_argument("-t", "--instance_type", default="c5.9xlarge",
                   metavar="EC2_TYPE", help="""EC2 instance type to use. Change
                   with caution. The instance type must have be able to attach
                   2 instance store devices.""")
    p.add_argument("-r", "--ramdisk_size", default="60", type=int,
                   metavar="GiB", help="""Size of ramdisk in GiB, limited by istance
                   RAM""")
    p.add_argument("-n", "--nocleanup", help="Don't cleanup resources.",
                   action="store_true", default=False)
    p.add_argument("-D", "--dryrun", action="store_true", default=False,
                   help="""Assign cruises to hosts but don't start instances.""")

    p.add_argument("--version", action="version", version="%(prog)s " + version)

    args = p.parse_args(args)

    return args


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    print("Started at {}".format(datetime.datetime.utcnow().isoformat()))

    # Print defined parameters
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    v["version"] = pkg_resources.get_distribution("seaflowpy").version
    print("Defined parameters:")
    print(json.dumps(v, indent=2))
    print("")

    # Make sure configuration for aws and ssh is ready to go
    config = conf.get_aws_config()
    conf.get_ssh_config(config)
    cloud = clouds.AWS(config.items("aws"))

    # Configure fabric
    env.connection_attempts = 6
    # Tell fabric the SSH user name and key file location
    env.user = config.get("ssh", "user")
    env.key_filename = os.path.expanduser(config.get("ssh", "ssh-key-file"))

    try:
        print("Getting lists of files for each cruise")
        cruise_files = {}
        try:
            for dbfile in args.dbs:
                # Make sure db has filter parameters filled in
                if not check_db_filter_params(dbfile):
                    print("No filter parameters found in database file {}".format(dbfile))
                    sys.exit(1)
                # Get cruise name from file name
                c = os.path.splitext(os.path.basename(dbfile))[0]
                cruise_files[c] = cloud.get_files(c)
                print("{:<20} {}".format(c, len(cruise_files[c])))
            print("")
        except botocore.exceptions.NoCredentialsError as e:
            print("Please configure aws first:")
            print("  $ conda install aws")
            print("  or")
            print("  $ pip install aws")
            print("  then")
            print("  $ aws configure")
            sys.exit(1)

        if args.dryrun:
            # Create dummy host list
            print("Creating {} dummy hosts".format(args.instance_count))
            env.hosts = ["dummy{}".format(i) for i in range(args.instance_count)]
        else:
            print("Starting {} instances".format(args.instance_count))
            result = cloud.start(
                count=args.instance_count,
                instance_type=args.instance_type
            )
            for iid, iip in zip(result["InstanceIds"], result["publicips"]):
                print("  InstanceId = {}, IP = {}".format(iid, iip))
            env.hosts.extend(result["publicips"])
        print("")

        # Fairly divide cruises into hosts based on number of files
        print("Assigning cruises to {} hosts".format(len(env.hosts)))
        host_assignments = assign_keys_to_hosts(env.hosts, cruise_files)
        for h in host_assignments:
            htotal = sum([c[1] for c in host_assignments[h]])
            print("{:<20} {}".format(h, htotal))
            for c in host_assignments[h]:
                print("  {:<18} {}".format(c[0], c[1]))
        print("")

        if args.dryrun:
            print("Dry run complete")
            print("")
            return

        print("Waiting for hosts to come up with SSH")
        execute(wait_for_up)

        print("Creating initial ramdisk")
        with hide("output"):
            execute(create_ramdisk, args.ramdisk_size)

        print("Transfer AWS credentials")
        with hide("output"):
            execute(rsync_put, ["~/.aws/"], ".aws")

        print("Transfer seaflowpy configuration")
        with hide("output"):
            execute(rsync_put, ["~/.seaflowpy/"], ".seaflowpy")

        print("Transfer initial databases")
        execute(mkdir, REMOTE_DB_DIR)  # create db dir on each host
        with hide("output"):
            execute(rsync_put, args.dbs, REMOTE_DB_DIR)

        print("Install seaflowpy")
        execute(pull_seaflowpy)

        # Host list in env.hosts should be populated now and all machines up
        print("Filter data")
        execute(filter_cruise, host_assignments, args.output_dir, args.process_count)
    finally:
        disconnect_all()  # always disconnect SSH connections
        if not args.nocleanup:
            cloud.cleanup()  # clean up in case of any unhandled exceptions
        print("Finished at {}".format(datetime.datetime.utcnow().isoformat()))


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
        fewest = {"host": None, "count": None}
        for h in hosts:
            # What is count of things assigned to this host plus this new item?
            total = sum([x[1] for x in assignments[h]]) + n
            # If this total has the fewest items so far, record this host
            # assignment
            if fewest["host"] is None or total < fewest["count"]:
                fewest["host"] = h
                fewest["count"] = total
        assignments[fewest["host"]].append((k, n))

    return assignments


@task
@parallel
def wait_for_up():
    # Try to run hostname to establish host is up and SSH is running
    with hide("everything"):
        # don't wait longer than 10 minutes
        run("hostname", timeout=600)


@task
@parallel
def create_ramdisk(gigabytes):
    # Make ramdisk at REMOTE_WORK_DIR
    with hide("everything"):
        # don't wait longer than 10 minutes
        run("sudo mkdir -p {}".format(REMOTE_WORK_DIR), timeout=600)
        run("sudo mount -t tmpfs -o size={}G tmpfs {}".format(gigabytes, REMOTE_WORK_DIR), timeout=600)


@task
@parallel
def rsync_put(localpaths, remotepath):
    # Delete to remote
    rsynccmd = [
        "rsync", "-au", "--stats", "--delete", "-e",
        "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename)
    ]
    rsynccmd.extend(localpaths)
    rsynccmd.append("{}@{}:{}".format(env.user, env.host_string, remotepath))
    result = local(" ".join(rsynccmd), capture=True)
    return result


@task
@parallel
def rsync_get(remotepath, localpath):
    # no delete to local
    rsynccmd = [
        "rsync", "-au", "--stats", "-e",
        "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename),
        "{}@{}:{}".format(env.user, env.host_string, remotepath),
        localpath
    ]
    result = local(" ".join(rsynccmd), capture=True)
    return result

@task
@parallel
def mkdir(d):
    with quiet():
        if run("test -d {}".format(d)).failed:
            run("mkdir {}".format(d))

@task
@parallel
def pull_seaflowpy():
    gitdir = "/home/ubuntu/git"
    repodir = os.path.join(gitdir, "seaflowpy")
    with quiet():
        if run("test -d {}".format(gitdir)).failed:
            run("mkdir {}".format(gitdir))
        if run("test -d {}".format(repodir)).failed:
            with show("running", "warnings", "stderr"):
                run("git clone https://github.com/armbrustlab/seaflowpy {}".format(repodir))
    with cd(repodir), hide("stdout"):
        run("git pull")
        run("python setup.py install --user")
        #run("python setup.py test")
        run("py.test")
        with show("stdout"):
            run("seaflowpy_filter --version")

@task
@parallel
def filter_cruise(host_assignments, output_dir, process_count=16):
    cruises = [x[0] for x in host_assignments[env.host_string]]
    cruise_results = {}
    with cd(REMOTE_WORK_DIR):
        for c in cruises:
            puts("Filtering cruise {}".format(c))
            with hide("commands"):
                run("mkdir {}".format(c))
            with hide("commands"):
                run("cp {}/{}.db {}".format(REMOTE_DB_DIR, c, c))
            with cd(c):
                text = {
                    "cruise": c,
                    "process_count": process_count
                }
                with settings(warn_only=True), hide("output"):
                    result = run(
                        "seaflowpy_filter --s3 -c {cruise} -d {cruise}.db -p {process_count} -o {cruise}_opp".format(**text),
                        timeout=10800
                    )
                    cruise_results[c] = result

            puts(result)

            cruise_output_dir = os.path.join(output_dir, c)

            if result.succeeded:
                puts("Filtering successfully completed for cruise {}".format(c))
                puts("Returning results for cruise {}".format(c))
                util.mkdir_p(cruise_output_dir)
                rsyncout = execute(
                    # rsync files in cruise results dir to local cruise dir
                    rsync_get,
                    os.path.join(REMOTE_WORK_DIR, c) + "/",
                    cruise_output_dir,
                    hosts=[env.host_string]
                )
                # Print rsync output on source host, even though this is run
                # on local, just to make it clear in logs which host is being
                # transferred from
                puts(rsyncout[env.host_string])
            else:
                sys.stderr.write("Filtering failed for cruise {}\n".format(c))

            # Always write log output
            util.mkdir_p(cruise_output_dir)
            logpath = os.path.join(cruise_output_dir, "seaflowpy_filter.{}.log".format(c))
            with open(logpath, "w") as logfh:
                logfh.write("command={}\n".format(result.command))
                logfh.write("real_command={}\n".format(result.real_command))
                logfh.write(result + "\n")

    return cruise_results


if __name__ == "__main__":
    main()
