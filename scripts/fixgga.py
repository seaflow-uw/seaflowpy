#!/usr/bin/env python3
# Convert GGA lat/lon columns to decimal degree
#
# This script is not polished and most likely needs to be edited to be useful.
# It's included in this project as a rough template for converting GGA coords
# to decimal degrees in a TSV or CSV file.
import sys
import seaflowpy as sfp

lines = []
with open(sys.argv[1], mode="r", encoding="utf-8") as fh:
    for x in fh:
        lines.append(x.rstrip().split(','))

print(",".join(lines[0]))
for x in lines[1:]:
    # Change columns here as needed
    x[2] = sfp.geo.ggalat2dd(x[2])
    x[3] = sfp.geo.ggalon2dd(x[3])
    print(",".join(x))
