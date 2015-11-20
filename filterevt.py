from argparse import ArgumentParser
import os
import sys
sys.path.append(os.path.join(os.environ["HOME"], "git/seaflowpy"))
import evtops
import seaflowsqlite3 as ssq


def main():
    p = ArgumentParser(description="Filter EVT data.")
    p.add_argument("-f", "--files", required=True, nargs="+",
                   help="EVT file paths. - to read from stdin.")
    p.add_argument("-i", "--input",
                   help="file listing EVT file paths. - to read from stdin.")
    p.add_argument("-d", "--db", required=True, help="sqlite3 db file")
    p.add_argument("-c", "--cruise", required=True, help="cruise name")
    p.add_argument("-n", "--notch", type=float, default=1.0,
                   help="notch size")
    p.add_argument("-w", "--width", type=float, default=0.5,
                   help="width size")
    p.add_argument("-s", "--slope", type=float, default=1.0,
                   help="slope value")

    args = p.parse_args()
    files = parse_file_list(args.files)
    filter_files(files, args.db, args.cruise, args.notch, args.width,
                 args.slope)


def parse_file_list(files):
    files_list = []
    if len(files) and files[0] == "-":
        for line in sys.stdin:
            files_list.append(line.rstrip())
    else:
        files_list = files
    exists = []
    for f in files_list:
        if not os.path.isfile(f):
            sys.stderr.write("%s does not exist\n" % f)
        else:
            exists.append(f)
    return exists


def filter_files(files, db, cruise, notch, width, slope):
    ssq.ensure_opp_table(db)
    ssq.ensure_opp_evt_ratio_table(db)
    evtcnt = 0
    oppcnt = 0
    for f in files:
        evt = evtops.EVT(f)
        evt.filter_particles(notch, width=width, slope=slope)
        evt.add_extra_columns(cruise, oppcnt)
        evt.write_opp_sqlite3(db)
        evt.write_opp_evt_ratio_sqlite3(cruise, db)
        evtcnt += evt.evtcnt
        oppcnt += evt.oppcnt
        print "%s: %i => %i (%.06f)" % (f, evt.evtcnt, evt.oppcnt,
                                        evt.opp_evt_ratio)
    try:
        opp_evt_ratio = float(oppcnt) / evtcnt
    except ZeroDivisionError:
        opp_evt_ratio = 0.0
    print "%s => %s (%.06f)\n" % (evtcnt, oppcnt, opp_evt_ratio)


if __name__ == "__main__":
    main()
