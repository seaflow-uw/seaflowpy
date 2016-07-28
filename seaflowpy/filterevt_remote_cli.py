#!/usr/bin/env python
import argparse
import botocore
import clouds
import conf
import datetime
import json
import os
import pkg_resources
import sys
import util
from fabric.api import (cd, env, execute, get, hide, local, parallel, puts,
    quiet, run, settings, show, task)
from fabric.network import disconnect_all


def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A program to filter EVT data on remote servers (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-c", "--cruises", nargs="+", required=True, metavar="NAME",
                   help="Cruise names (required)")

    p.add_argument("-o", "--output_dir", metavar="DIR", required=True,
                   help="""Directory in which to save SQLite3 database and
                   OPP binary files for each cruise. Will be created if does
                   not exist.""")

    p.add_argument("-p", "--process_count", required=False, type=int, default=16,
                   metavar="N", help="""Number of processes to use in filtering
                   (optional)""")
    p.add_argument("-i", "--instance_count", required=False, type=int, default=1,
                   metavar="N", help="""Number of cloud instances to use
                   (optional)""")
    p.add_argument("-t", "--instance_type", required=False, default="c3.8xlarge",
                   metavar="EC2_TYPE", help="""EC2 instance type to use. Change
                   with caution. The instance type must have be able to attach
                   2 instance store devices. (optional)""")
    p.add_argument("-n", "--nocleanup", help="Don't cleanup resources",
                   action="store_true", default=False)

    p.add_argument("--version", action="version", version="%(prog)s " + version)

    args = p.parse_args(args)

    return args


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    print "Started at {}".format(datetime.datetime.utcnow().isoformat())

    # Print defined parameters
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    v["version"] = pkg_resources.get_distribution("seaflowpy").version
    print "Defined parameters:"
    print json.dumps(v, indent=2)
    print ""

    # Make sure configuration for s3 is ready to go
    config = conf.get_aws_config(s3_only=True)
    cloud = clouds.AWS(config.items("aws"))

    # Configure fabric
    env.connection_attempts = 6
    # Tell fabric the SSH user name and key file location
    env.user = config.get("ssh", "user")
    env.key_filename = os.path.expanduser(config.get("ssh", "ssh-key-file"))

    try:
        print "Getting lists of files for each cruise"
        cruise_files = {}
        try:
            for c in args.cruises:
                cruise_files[c] = cloud.get_files(c)
                print "{:<20} {}".format(c, len(cruise_files[c]))
        except botocore.exceptions.NoCredentialsError as e:
            print "Please configure aws first:"
            print "  $ conda install aws"
            print "  or"
            print "  $ pip install aws"
            print "  then"
            print "  $ aws configure"
            sys.exit(1)

        print "Starting {} instances".format(args.instance_count)
        result = cloud.start(
            count=args.instance_count,
            instance_type=args.instance_type
        )
        for iid, iip in zip(result["InstanceIds"], result["publicips"]):
            print "  InstanceId = {}, IP = {}".format(iid, iip)
        env.hosts.extend(result["publicips"])

        # Fairly divide cruises into hosts based on number of files
        print "Assigning cruises to {} hosts".format(len(env.hosts))
        host_assignments = assign_keys_to_hosts(env.hosts, cruise_files)
        for h in host_assignments:
            htotal = sum([c[1] for c in host_assignments[h]])
            print "{:<20} {}".format(h, htotal)
            for c in host_assignments[h]:
                print "  {:<18} {}".format(c[0], c[1])

        print "Waiting for hosts to come up with SSH"
        execute(wait_for_up)

        print "Transfer AWS credentials"
        with hide("output"):
            execute(rsync_put, "~/.aws/", ".aws")

        print "Transfer seaflowpy configuration"
        with hide("output"):
            execute(rsync_put, "~/.seaflowpy/", ".seaflowpy")

        print "Install seaflowpy"
        execute(pull_seaflowpy)

        # Host list in env.hosts should be populated now and all machines up
        print "Filter data"
        execute(filter_cruise, host_assignments, args.output_dir, args.process_count)
    finally:
        disconnect_all()  # always disconnect SSH connections
        if not args.nocleanup:
            cloud.cleanup()  # clean up in case of any unhandled exceptions
        print "Finished at {}".format(datetime.datetime.utcnow().isoformat())


def count_things(data):
    counts = [(k, len(v)) for k, v in data.items()]
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
        run("hostname", timeout=600)  # don't wait longer than 10 minutes


@task
@parallel
def rsync_put(localpath, remotepath):
    rsynccmd = ["rsync", "-au", "--stats", "--delete"]  # delete to remote
    remotepath = "{}@{}:{}".format(env.user, env.host_string, remotepath)
    rsynccmd.extend(
        [
            "-e",
            "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename),
            localpath, remotepath
        ]
    )
    result = local(" ".join(rsynccmd), capture=True)
    return result


@task
@parallel
def rsync_get(remotepath, localpath):
    rsynccmd = ["rsync", "-au", "--stats"]  # no delete to local
    remotepath = "{}@{}:{}".format(env.user, env.host_string, remotepath)
    rsynccmd.extend(
        [
            "-e",
            "'ssh -i {} -o StrictHostKeyChecking=no'".format(env.key_filename),
            remotepath, localpath
        ]
    )
    result = local(" ".join(rsynccmd), capture=True)
    return result


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
        run("python setup.py install")
        run("python setup.py test")
        with show("stdout"):
            run("seaflowpy_filter --version")

@task
@parallel
def filter_cruise(host_assignments, output_dir, process_count=16):
    cruises = [x[0] for x in host_assignments[env.host_string]]
    cruise_results = {}
    workdir = "/mnt/raid"
    with cd(workdir):
        for c in cruises:
            puts("Filtering cruise {}".format(c))
            with hide("commands"):
                run("mkdir {}".format(c))
            with cd(c):
                text = {"cruise": c, "process_count": process_count}
                with settings(warn_only=True), hide("output"):
                    #result = run("seaflowpy_filter --s3 -c {cruise} -d {cruise}.db -l 10 -p 2 -o {cruise}_opp".format(**text))
                    result = run(
                        "seaflowpy_filter --s3 -c {cruise} -d {cruise}.db -t -p {process_count} -o {cruise}_opp".format(**text),
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
                    os.path.join(workdir, c) + "/",
                    cruise_output_dir,
                    hosts=[env.host_string]
                )
                # Print rsync output on source host, even though this is run
                # on local
                puts(rsyncout[env.host_string])
            else:
                warn("Filtering failed for cruise {}".format(c))

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
