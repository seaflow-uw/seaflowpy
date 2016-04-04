#!/usr/bin/env python
import argparse
import aws
import db
import evt
import filterevt
import botocore
import pprint
import sys

# Global configuration variables for AWS
# ######################################
# Default name of Seaflow bucket
SEAFLOW_BUCKET = "armbrustlab.seaflow"

def parse_args(args):
    p = argparse.ArgumentParser(
        description="Filter EVT data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    g_in = p.add_mutually_exclusive_group(required=True)
    g_in.add_argument("--files", nargs="+",
                   help="""EVT file paths. - to read from stdin.
                        (required unless --evt_dir or --s3)""")
    g_in.add_argument("--evt_dir",
                   help="EVT directory path (required unless --files or --s3)")
    g_in.add_argument("--s3", default=False, action="store_true",
                   help="""Read EVT files from s3://S3_BUCKET/CRUISE where
                        cruise is provided by --cruise (required unless --files
                        or --evt_dir)""")

    p.add_argument("--db", required=True,
                   help="""SQLite3 db file. (required)""")
    p.add_argument("--opp_dir",
                   help="""Directory in which to save LabView binary formatted
                        files of focused particles (OPP). Will be created
                        if does not exist. (optional)""")

    p.add_argument("--cruise", required=True, help="Cruise name (required)")
    p.add_argument("--notch1", type=float, help="Notch 1 (optional)")
    p.add_argument("--notch2", type=float, help="Notch 2 (optional)")
    p.add_argument("--width", type=float, default=0.5, help="Width (optional)")
    p.add_argument("--origin", type=float, help="Origin (optional)")
    p.add_argument("--offset", type=float, default=0.0,
                   help="Offset (optional)")

    p.add_argument("--cpus", required=False, type=int, default=1,
                   help="""Number of CPU cores to use in filtering
                        (optional)""")
    p.add_argument("--progress", type=float, default=10.0,
                   help="Progress update %% resolution (optional)")
    p.add_argument("--limit", type=int, default=None,
                   help="""Limit how many files to process. Useful for testing.
                        (optional)""")
    p.add_argument("--s3_bucket", default=SEAFLOW_BUCKET,
                   help="S3 bucket name (optional)")

    return p.parse_args(args)


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    # Print defined parameters
    v = dict(vars(args))
    to_delete = [k for k in v if v[k] is None]
    for k in to_delete:
        v.pop(k, None)  # Remove undefined parameters
    print "\nDefined parameters:"
    pprint.pprint(v, indent=2)

    # Find EVT files
    if args.files:
        files = evt.parse_evt_file_list(args.files)
    elif args.evt_dir:
        files = evt.find_evt_files(args.evt_dir)
    elif args.s3:
        # Make sure try to access S3 up front to setup AWS credentials before
        # launching child processes.
        try:
            files = aws.get_s3_files(args.cruise, args.s3_bucket)
            files = evt.parse_evt_file_list(files)  # Only keep EVT files
        except botocore.exceptions.NoCredentialsError as e:
            print "Please configure aws first:"
            print "  $ conda install aws"
            print "  or"
            print "  $ pip install aws"
            print "  then"
            print "  $ aws configure"
            sys.exit(1)

    # Restrict length of file list with --limit
    if (not args.limit is None) and (args.limit > 0):
        files = files[:args.limit]

    # Copy --progress to --every alias
    args.every = args.progress

    # Construct kwargs to pass to filter
    kwargs = vars(args)
    filter_keys = ["notch1", "notch2", "width", "offset", "origin"]
    kwargs["filter_options"] = dict((k, kwargs[k]) for k in filter_keys)
    kwargs["files"] = files

    # Filter
    filterevt.filter_evt_files(**kwargs)
    # Index
    if args.db:
        db.ensure_indexes(args.db)


if __name__ == "__main__":
    main()
