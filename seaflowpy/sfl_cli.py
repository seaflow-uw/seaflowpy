"""Modify, validate, and import SFl files"""
from __future__ import absolute_import
from __future__ import unicode_literals
from . import sfl
import argparse
import os
import pkg_resources
import sys


def create_parser():
    version = pkg_resources.get_distribution("seaflowpy").version

    # PARENT with input options
    parent_parser = argparse.ArgumentParser(add_help=False)
    input_group = parent_parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "-S", "--sfl_dir",
        help="""Directory containing SFL files. Will be searched recursively."""
    )
    input_group.add_argument(
        "-s", "--sfl",
        nargs="+",
        dest="sfl_files",
        help="""SFL files if EVT directory is not provided."""
    )

    # PARSER
    parser = argparse.ArgumentParser(
        description="Modify, validate, and import SFl files. (%s)" % version
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + version
    )

    subparsers = parser.add_subparsers(dest="cmd")

    # CHECK
    parser_check = subparsers.add_parser(
        "check", parents=[parent_parser],
        help="Validate SFL file data",
        description="""Check SFL file validity. Report duplicate files, non-UTC
        timestamps, missing values in required columns, bad coordinates. Only
        the first error of each type will be reported."""
    )
    parser_check.add_argument(
        "-a", "--all_errors",
        action="store_true",
        help="""Print all errors."""
    )
    parser_check.add_argument(
        "-H", "--error_header",
        action="store_true",
        help="""Print header line for tsv error output."""
    )
    parser_check.add_argument(
        "-j", "--json_errors",
        action="store_true",
        help="""Print errors as JSON."""
    )
    parser_check.set_defaults(func=do_check)

    # CONVERT_GGA
    parser_convert_gga = subparsers.add_parser(
        "convert_gga", parents=[parent_parser],
        help="Convert GGA coordinates to decimal degrees",
        description="""Convert GGA coordinates in SFL file to to decimal
        degrees."""
    )
    parser_convert_gga.add_argument(
        "-o", "--outfile",
        help="""Tab-delimited output file. "-" or unspecified for stdout."""
    )
    parser_convert_gga.set_defaults(func=do_convert_gga)

    # DB
    parser_db = subparsers.add_parser(
        "db", parents=[parent_parser],
        help="Import SFL file data to database",
        description="""Write processed SFL file data to SQLite3 database files.
        Data will be checked before inserting. If any errors are found the
        first of each type will be reported and no data will be written."""
    )
    parser_db.add_argument(
        "-a", "--all_errors",
        action="store_true",
        help="""Print all errors."""
    )
    parser_db.add_argument(
        "-c", "--cruise",
        help="""Cruise name."""
    )
    parser_db.add_argument(
        "-d", "--db",
        required=True,
        help="""Path to SQLite3 database file."""
    )
    parser_db.add_argument(
        "-f", "--force",
        action="store_true",
        help="""Attempt DB import even if validation produces errors"""
    )
    parser_db.add_argument(
        "-H", "--error_header",
        action="store_true",
        help="""Print header line for tsv error output."""
    )
    parser_db.add_argument(
        "-j", "--json_errors",
        action="store_true",
        help="""Print errors as JSON."""
    )
    parser_db.set_defaults(func=do_db)

    # DEDUP
    parser_dedup = subparsers.add_parser(
        "dedup", parents=[parent_parser],
        help="Remove duplicate 'FILE' lines",
        description="""Remove lines with duplicate file entries. Print files
        removed to stderr."""
    )
    parser_dedup.add_argument(
        "-o", "--outfile",
        help="""Tab-delimited output file. "-" or unspecified for stdout."""
    )
    parser_dedup.set_defaults(func=do_dedup)

    # DETECT_GGA
    parser_detect_gga = subparsers.add_parser(
        "detect_gga", parents=[parent_parser],
        help="Detect GGA coordinates",
        description="""Detect rows with GGA lat and lon coordinates. If any are
        found, print 'True' and exit with status 0. Otherwise print 'False' and
        exit with non-zero exit status."""
    )
    parser_detect_gga.set_defaults(func=do_detect_gga)

    # PRINT
    parser_print = subparsers.add_parser(
        "print", parents=[parent_parser],
        help="Print processed SFL file",
        description="""Print processed SFL file data to stdout. Output columns will
        match columns selected for database import."""
    )
    parser_print.add_argument(
        "-c", "--cruise",
        help="""Cruise name."""
    )
    parser_print.add_argument(
        "-o", "--outfile",
        help="""Tab-delimited output file. "-" or unspecified for stdout."""
    )
    parser_print.set_defaults(func=do_print)

    return parser


def main(cli_args):
    """Main function to implement command-line interface"""
    parser = create_parser()
    args = parser.parse_args(cli_args)

    if args.cmd is None:
        parser.print_help()
        status = 0
    else:
        # Find input files
        if args.sfl_dir:
            args.sfl_files = sfl.find_sfl_files(args.sfl_dir)

        # Set default output file to stdout
        outfile = getattr(args, "outfile", None)
        if outfile is None or outfile == "-":
            args.outfile = sys.stdout

        # Call the subcommand function
        status = args.func(args)

    return status


def do_check(args):
    df = sfl.read_files(args.sfl_files)

    df = sfl.fix(df)
    errors = sfl.check(df)
    # Remove cruise errors from here
    errors = [e for e in errors if e["column"] != "cruise"]

    if len(errors) > 0:
        if args.json_errors:
            sfl.print_json_errors(errors, sys.stdout, print_all=args.all_errors)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=args.all_errors, header=args.error_header)
        return 1


def do_convert_gga(args):
    df = sfl.read_files(args.sfl_files, convert_numerics=False)

    df = sfl.convert_gga2dd(df)

    sfl.save_to_file(df, args.outfile, all_columns=True)


def do_db(args):
    df = sfl.read_files(args.sfl_files)

    if args.cruise is not None:
        df = sfl.add_cruise(df, args.cruise)
    df = sfl.fix(df)
    errors = sfl.check(df)

    if len(errors) > 0:
        if args.json_errors:
            sfl.print_json_errors(errors, sys.stdout, print_all=args.all_errors)
        else:
            sfl.print_tsv_errors(errors, sys.stdout, print_all=args.all_errors, header=args.error_header)
        if not args.force:
            return 1
    sfl.save_to_db(df, args.db)


def do_dedup(args):
    df = sfl.read_files(args.sfl_files, convert_numerics=False)

    dup_files, df = sfl.dedup(df)

    if len(dup_files):
        sys.stderr.write("\n".join(["{}\t{}".format(*d) for d in dup_files]) + "\n")
    sfl.save_to_file(df, args.outfile, all_columns=True)


def do_detect_gga(args):
    df = sfl.read_files(args.sfl_files, convert_numerics=False)

    # Has any GGA coordinates?
    if sfl.has_gga(df):
        print("True")
        return 0
    else:
        print("False")
        return 1


def do_print(args):
    df = sfl.read_files(args.sfl_files)

    if args.cruise is not None:
        df = sfl.add_cruise(df, args.cruise)
    df = sfl.fix(df)

    sfl.save_to_file(df, args.outfile, convert_colnames=True)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
