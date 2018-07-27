#!/usr/bin/env python
from __future__ import print_function
import argparse
from . import errors
from . import evt
from . import seaflowfile
from . import sfl
import json
import os
import pkg_resources
import sys


def create_parser():
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A program to confirm existence of files listed in SFL. (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-e", "--evt_dir", metavar="DIR", required=True,
                   help="EVT directory path")
    p.add_argument("-s", "--sfl", metavar="FILE", required=True,
                   help="SFL file for EVT directory")
    p.add_argument("-v", "--verbose", default=False, action="store_true",
                   help="""Print a list of all file ids no in common between SFL
                   and directory.""")

    p.add_argument("--version", action="version", version="%(prog)s " + version)

    return p


def main(cli_args=None):
    """Main function to implement command-line interface"""
    parser = create_parser()
    args = parser.parse_args(cli_args)
    found_evt_ids = []
    for e in evt.find_evt_files(args.evt_dir):
        found_evt_ids.append(seaflowfile.SeaFlowFile(e).path_file_id)

    sfl_df = sfl.read_files([args.sfl])
    sfl_evt_ids = []
    for e in sfl_df["file"]:
        sfl_evt_ids.append(seaflowfile.SeaFlowFile(e).file_id)

    sfl_set = set(sfl_evt_ids)
    found_set = set(found_evt_ids)

    print("%d in SFL file %s" % (len(sfl_set), args.sfl))
    print("%d files in directory %s" % (len(found_set), args.evt_dir))
    print("%d files in common" % len(sfl_set.intersection(found_set)))
    if args.verbose and \
       (len(sfl_set.intersection(found_set)) != len(sfl_set) or
        len(sfl_set.intersection(found_set)) != len(found_set)):
        print("\n")
        print("Files in SFL but not discovered:")
        print("\n".join(sorted(sfl_set.difference(found_set))))
        print("\n")
        print("Files discovered but not in SFL:")
        print("\n".join(sorted(found_set.difference(sfl_set))))
        print("\n")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
