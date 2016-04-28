"""Export sfl table or stat table/view as CSV."""
import argparse
import pkg_resources
import subprocess
import sys


def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version
    parser = argparse.ArgumentParser(
        description="""Export sfl table or stat table/view as CSV.""")
    parser.add_argument("db", help="Popcycle V1 SQLite3 DB file")
    parser.add_argument("table", choices=["sfl", "stats"],
        help="Name of table/view to output")
    parser.add_argument("--version", action="version", version="%(prog)s " + version)

    return parser.parse_args()


def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    cmd = ["sqlite3", "-header", "-csv", args.db]
    if args.table == "stats":
        sql =  "SELECT * FROM {} ORDER BY cruise, file, pop ASC".format(args.table)
    elif args.table == "sfl":
        sql =  "SELECT * FROM {} ORDER BY cruise, file ASC".format(args.table)
    else:
        raise ValueError("table must be sfl or stats")
    cmd.append(sql)
    subprocess.check_call(cmd)


if __name__ == "__main__":
    main()
