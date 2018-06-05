#!/usr/bin/env python
from __future__ import print_function
import argparse
import botocore
from . import clouds
from . import conf
from . import db
from . import errors
from . import evt
from . import filterevt
import json
import pkg_resources
import sys


def create_parser():
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A program to filter EVT data (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-e", "--evt_dir", metavar="DIR",
                   help="EVT directory path (required unless --s3)")
    p.add_argument("-s", "--s3", default=False, action="store_true",
                   help="""Read EVT files from s3://S3_BUCKET/CRUISE where
                   CRUISE is detected in the sqlite db metadata table
                   (required unless --evt_dir)""")
    p.add_argument("-l", "--limit", type=int, default=None, metavar="N",
                   help="""Limit how many files to process. Useful for testing.
                   (optional)""")

    p.add_argument("-d", "--db", required=True, metavar="FILE",
                   help="""SQLite3 db file. (required)""")
    p.add_argument("-o", "--opp_dir", metavar="DIR",
                   help="""Directory in which to save LabView binary formatted
                   files of focused particles (OPP). Will be created
                   if does not exist. (optional)""")

    p.add_argument("-p", "--process_count", required=False, type=int, default=1,
                   metavar="N", help="""Number of processes to use in filtering
                   (optional)""")
    p.add_argument("-r", "--resolution", type=float, default=10.0, metavar="N",
                   help="Progress update resolution by %% (optional)")

    p.add_argument("--version", action="version", version="%(prog)s " + version)

    return p


def main(cli_args=None):
    """Main function to implement command-line interface"""
    parser = create_parser()
    args = parser.parse_args(cli_args)

    # Validate args
    if not args.evt_dir and not args.s3:
        sys.stderr.write("Error: One of --evt_dir or --s3 must be provided\n")
        parser.print_help()
        return 1

    # TODO maybe import sfl here?

    # Find cruise in db
    try:
        args.cruise = db.get_cruise(args.db)
    except errors.SeaflowpyError as e:
        sys.stderr.write("Error: {}\n".format(e))
        return 1

    # Find filter parameters in db. Won't use them yet but better to check
    # upfront
    try:
        filter_params = db.get_latest_filter(args.db)
    except errors.SeaflowpyError as e:
        sys.stderr.write("Error: {}\n".format(e))
        return 1

    # Capture software version
    args.version = pkg_resources.get_distribution("seaflowpy").version

    # Convert parameters into dictionary
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters

    # Print run parameters
    print("Defined parameters:")
    print(json.dumps(v, indent=2))
    print("")

    # Find EVT files
    if args.evt_dir:
        files = evt.find_evt_files(args.evt_dir)
    elif args.s3:
        # Make sure configuration for s3 is ready to go
        config = conf.get_aws_config(s3_only=True)
        cloud = clouds.AWS(config.items("aws"))
        # Make sure try to access S3 up front to setup AWS credentials before
        # launching child processes.
        try:
            files = cloud.get_files(args.cruise)
            files = evt.parse_file_list(files)  # Only keep EVT files
        except botocore.exceptions.NoCredentialsError as e:
            print("Please configure aws first:")
            print("  $ conda install aws")
            print("  or")
            print("  $ pip install aws")
            print("  then")
            print("  $ aws configure")
            return 1

    # Restrict length of file list with --limit
    if (not args.limit is None) and (args.limit > 0):
        files = files[:args.limit]

    # Filter
    try:
        filterevt.filter_evt_files(files, args.db, args.opp_dir, s3=args.s3,
                                   process_count=args.process_count,
                                   every=args.resolution)
    except errors.SeaflowpyError as e:
        sys.stderr.write("Error: {}\n".format(e))
        return 1

    # Index
    if args.db:
        db.ensure_indexes(args.db)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
