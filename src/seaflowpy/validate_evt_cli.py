#!/usr/bin/env python
from __future__ import print_function
import argparse
from . import errors
from . import evt
import json
import pkg_resources
import sys


def create_parser():
    version = pkg_resources.get_distribution("seaflowpy").version

    p = argparse.ArgumentParser(
        description="A program to validate EVT files (version %s)" % version,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-v", "--verbose", default=False, action="store_true",
                   help="""Show status for all files. If not specified then only
                   files with errors are printed.""")
    p.add_argument("files", metavar="FILE", nargs="+",
                   help="EVT files")

    p.add_argument("--version", action="version", version="%(prog)s " + version)

    return p


def main(cli_args=None):
    """Main function to implement command-line interface"""
    parser = create_parser()
    args = parser.parse_args(cli_args)
    ok, bad = 0, 0
    for evt_file in args.files:
        if not evt.is_evt(evt_file):
            status = "Filename does not look like an EVT file"
            bad += 1
        else:
            try:
                _ = evt.EVT(path=evt_file)
            except errors.EVTFileError as e:
                status = e
                bad += 1
            else:
                status = "OK"
                ok += 1
        if args.verbose or status != "OK":
            print("%s: %s" % (evt_file, status))
    print("%d/%d files passed validation" % (ok, bad + ok))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
