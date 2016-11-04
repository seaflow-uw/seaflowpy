#!/usr/bin/env python
"""
Convert old Seaflow SDS file format to SFL, with STREAM PRESSURE converted
to FLOW RATE with user supplied ratio.
"""
import datetime
import geo
import pkg_resources
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

# key = instrument serial number
# value = ratio_evt_stream for that instrument
SERIALS = {
    "740": 0.136,
    "751": 0.143,
    "989": 0.149
}

FILE_COLUMNS = [
    'FILE', 'DATE', 'FILE DURATION', 'LAT', 'LON', 'CONDUCTIVITY',
    'SALINITY', 'OCEAN TEMP', 'PAR', 'BULK RED', 'STREAM PRESSURE',
    'FLOW RATE', 'EVENT RATE'
]


def parse_header(line):
    """Create a dict of column indexes by field name"""
    fields = line.rstrip().split("\t")
    fields = [f.replace(".", " ") for f in fields]
    d = dict([(x, i) for i, x in enumerate(fields)])
    return d

def create_file_field(line, header):
    """Create SeaFlow file name form this line"""
    file_field = None
    fields = line.rstrip().split("\t")
    if ("file" in header) and ("day" in header):
        filei = header["file"]
        dayi = header["day"]
        file_field = fields[dayi] + "/" + fields[filei] + ".evt"
    elif "FILE" in header:
        filei = header["FILE"]
        if fields[filei].startswith("sds_"):
            parts = fields[filei].split("_")
            file_field = parts[1] + "_" + parts[2] + "/" + parts[3] + ".evt"
    if not file_field:
        sys.stderr.write("Error: could not create file name from this line:\n")
        sys.stderr.write(line)
        sys.exit(1)
    return file_field

def create_date_field(line, header):
    """Create ISO8601 date value from this line"""
    date_field = None
    fields = line.rstrip().split("\t")
    if "time" in header:
        timei = header["time"]
        date_field = fields[timei].replace(" ", "T") + "+00:00"  # assume UTC
    elif "computerUTC" in header:
        # Given the following definitions:
        # y = year
        # j = julian day
        # h = hour
        # m = minute
        # s = second
        #
        # Then computerUTC is formatted as yyjjjhhmmss
        cutc = fields[header["computerUTC"]]
        assert len(cutc) == len("yyjjjhhmmss")
        year = int("20" + cutc[:2])
        julian = int(cutc[2:5])
        hours = int(cutc[5:7])
        minutes = int(cutc[7:9])
        seconds = int(cutc[9:11])

        # julian to month/day
        delta = datetime.timedelta(days=julian-1, hours=hours, minutes=minutes,
                                   seconds=seconds)
        tmpd = datetime.datetime(year, 1, 1) + delta
        # Final UTC ISO8601 SeaFlow compatible timestamp string
        date_field = tmpd.isoformat()  + "+00:00"
    if not date_field:
        sys.stderr.write("Error: could not create date from this line:\n")
        sys.stderr.write(line)
        sys.exit(1)
    return date_field

def create_flow_rate_field(line, header, ratio_evt_stream):
    flow_rate_field = None
    fields = line.rstrip().split("\t")
    if "STREAM PRESSURE" in header:
        # Create a new FLOW RATE element from STREAM PRESSURE and ratio_evt_stream
        try:
            sp = float(fields[header["STREAM PRESSURE"]])
            flow_rate_field = str(1000 * (-9*10**-5 * sp**4 + 0.0066 * sp**3 - 0.173 * sp**2 + 2.5013 * sp + 2.1059) * ratio_evt_stream)
        except ValueError:
            flow_rate_field = "NA"
    if not flow_rate_field:
        sys.stderr.write("Error: could not create flow rate from this line:\n")
        sys.stderr.write(line)
        sys.exit(1)
    return flow_rate_field

def fix_coord(coord, gga=False, west=False):
    if coord:
        if gga:
            coord = geo.gga2dd(coord)
        if west:
            coord = geo.westify_dd_lon(coord)
    return coord

def parse_args(args):
    version = pkg_resources.get_distribution("seaflowpy").version

    parser = ArgumentParser(
        description="""Convert old Seaflow SDS file format to SFL, with STREAM
        PRESSURE converted to FLOW RATE with user supplied ratio (version %s)""" % version)
    parser.add_argument('--sds', required=True, help='Input SDS file')
    parser.add_argument('--sfl', required=True, help='Output SFL file.')
    parser.add_argument('--serial', required=True, help='Seaflow instrument serial number')
    parser.add_argument(
        '-g', "--gga", action='store_true',
        help='lat/lon input is in GGA format. Convert to decimal degree.')
    parser.add_argument(
        '-w', '--west', action='store_true',
        help="""Some ships don't provide E/W designations for longitude. Use
        this flag if this is the case and all longitudes should be West
        (negative).""")
    parser.add_argument("--version", action="version", version="%(prog)s " + version)

    return parser.parse_args()

def main(cli_args=None):
    """Main function to implement command-line interface"""
    if cli_args is None:
        cli_args = sys.argv[1:]

    args = parse_args(cli_args)

    try:
        ratio_evt_stream = SERIALS[args.serial]
    except KeyError:
        sys.stderr.write("Instrument serial number %s not recognized\n" % args.serial)
        sys.exit(1)

    f = open(args.sds)
    f2 = open(args.sfl, 'w')

    header = parse_header(f.readline())

    f2.write("\t".join(FILE_COLUMNS) + "\n")

    file_duration_field = "180"
    for line in f:
        file_field = create_file_field(line, header)
        date_field = create_date_field(line, header)
        flow_rate_field = create_flow_rate_field(line, header, ratio_evt_stream)

        # Capture original fields
        fields = line.rstrip().split("\t")
        # Add new or modified fields
        fields.append(file_duration_field)
        fields.append(file_field)
        fields.append(date_field)
        fields.append(flow_rate_field)

        # Make a copy of header index lookup
        d = dict(header)
        # Add indices for appended new or modified fields
        d["FILE DURATION"] = len(fields) - 4
        d["FILE"] = len(fields) - 3
        d["DATE"] = len(fields) - 2
        d["FLOW RATE"] = len(fields) - 1

        # Fix coordinates
        fields[d["LAT"]] = fix_coord(fields[d["LAT"]], gga=args.gga)
        fields[d["LON"]] = fix_coord(fields[d["LON"]], gga=args.gga, west=args.west)

        # Write SFL subset of fields in correct order
        outfields = []
        for col in FILE_COLUMNS:
            outfields.append(fields[d[col]])
        f2.write("\t".join(outfields) + "\n")

    f.close()
    f2.close()
