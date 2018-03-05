"""Import SFl file into sqlite3 database."""
from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import str
from . import seaflowfile
import argparse
import os
import pkg_resources
import sys

def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    parser = argparse.ArgumentParser(
        description='A program to get the julian day directory name from an EVT/OPP file name. (version %s)' % version)

    parser.add_argument(
        "paths", metavar="FILE", nargs="+",
        help="""EVT/OPP file path. Must be a new-style datestamped file. Any
        part of the file path except for the filename will be ignored. The
        filename may include the '.gz' extension.""")

    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Print 3 columns: input path, file name, julian dir."
    )

    parser.add_argument("--version", action="version", version="%(prog)s " + version)

    return parser.parse_args(args)


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    output = []
    for path in args.paths:
        sfile = seaflowfile.SeaFlowFile(path)
        if args.verbose:
            output.append([path, sfile.filename, sfile.julian])
        else:
            output.append([sfile.julian])
    print("\n".join(["\t".join(row) for row in output]))


if __name__ == "__main__":
    main()
