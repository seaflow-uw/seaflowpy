#!/usr/bin/env python
import argparse
import classifyopp
import db
import evt
import pkg_resources
import pprint
import seaflowfile
import sys


def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A Python program to classify OPP data (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-o", "--opp_dir", metavar="DIR", required=True,
                   help="OPP directory path. (required)")
    p.add_argument("-s", "--start", metavar="FILE",
                   help="First file to classify. (optional)")
    p.add_argument("-e", "--end", metavar="FILE",
                   help="Last file to classify. (optional)")
    p.add_argument("-l", "--limit", type=int, default=None, metavar="N",
                   help="""Limit how many files to process. Useful for testing.
                   (optional)""")

    p.add_argument("-g", "--gating_id", required=True, metavar="ID",
                   help="ID for gating parameters to use (required)")
    p.add_argument("-c", "--cruise", required=True, metavar="NAME",
                   help="Cruise name (required)")

    p.add_argument("-d", "--db", required=True, metavar="FILE",
                   help="SQLite3 db file. (required)")
    p.add_argument("-v", "--vct_dir", metavar="DIR",
                   help="""Directory in which to save VCT csv files of particle
                   population annotations. Will be created if does not exist.
                   (required)""")

    p.add_argument("-p", "--process_count", required=False, type=int, default=1,
                   metavar="N", help="""Number of processes to use in filtering.
                   (optional)""")
    p.add_argument("-r", "--resolution", type=float, default=10.0, metavar="N",
                   help="Progress update resolution by %% (optional)")

    p.add_argument("--version", action="version", version="%(prog)s " + version)

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
    v["version"] = pkg_resources.get_distribution("seaflowpy").version
    print "\nDefined parameters:"
    pprint.pprint(v, indent=2)


    # Find OPP files
    files = evt.find_evt_files(args.opp_dir)

    # Restrict to >= start and <= end
    if args.start:
        try:
            seaflowfile.find_file_index(files, args.start)
        except ValueError as e:
            print "\nWarning: %s not found in %s" % (args.start, args.opp_dir)
    if args.end:
        try:
            seaflowfile.find_file_index(files, args.end)
        except ValueError as e:
            print "\nWarning: %s not found in %s" % (args.end, args.opp_dir)
    files = seaflowfile.files_between(files, args.start, args.end)

    # Restrict length of file list with --limit
    if (not args.limit is None) and (args.limit > 0):
        files = files[:args.limit]

    # Classify
    classifyopp.classify_opp_files(files, args.cruise, args.gating_id,
                                   args.db, args.vct_dir,
                                   process_count=args.process_count,
                                   every=args.resolution)


if __name__ == "__main__":
    main()
