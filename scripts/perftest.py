#!/usr/bin/env python3
# Script to help test numpy performance.
# To assess parallelization scaling on linux, run variants of this command:
#
# parallel -P 8 'perf stat -e task-clock,cycles,instructions,context-switches,cpu-migrations,page-faults,cache-references,cache-misses python ~/git/seaflowpy-test/scripts/perftest.py -f SCOPE_14_evt/2016_068/*' ::: {1..8} 2>&1 | grep -v 'Performance'
#
# Adjust to scale from 1 core to N cores. Check for the spikes in cache misses
# as this is the major factor in scaling degradation. It usually happens when
# too many processes are assigned to the same CPU. e.g. 4 processes on a
# 2-socket 4-cores-per-socket system may show reasonable scaling, but 8
# processes may trigger much more cache contention.
import click
import os
import time
import pandas
import seaflowpy as sfp

filter_params = {'id': {0: '87ff0de6-88da-4d44-ab87-9feb955a72d6', 1: '87ff0de6-88da-4d44-ab87-9feb955a72d6', 2: '87ff0de6-88da-4d44-ab87-9feb955a72d6'}, 'date': {0: '2019-06-20T18:10:22+00:00', 1: '2019-06-20T18:10:22+00:00', 2: '2019-06-20T18:10:22+00:00'}, 'quantile': {0: 2.5, 1: 50.0, 2: 97.5}, 'beads_fsc_small': {0: 52368.0, 1: 53056.0, 2: 53552.0}, 'beads_D1': {0: 29296.0, 1: 28048.0, 2: 27024.0}, 'beads_D2': {0: 32608.0, 1: 29744.0, 2: 27260.0}, 'width': {0: 5000.0, 1: 5000.0, 2: 5000.0}, 'notch_small_D1': {0: 0.5589999999999999, 1: 0.529, 2: 0.505}, 'notch_small_D2': {0: 0.623, 1: 0.561, 2: 0.509}, 'notch_large_D1': {0: 1.64, 1: 1.635, 2: 1.63}, 'notch_large_D2': {0: 1.6369999999999998, 1: 1.632, 2: 1.6269999999999998}, 'offset_small_D1': {0: 0.0, 1: 0.0, 2: 0.0}, 'offset_small_D2': {0: 0.0, 1: 0.0, 2: 0.0}, 'offset_large_D1': {0: -56588.0, 1: -58699.0, 2: -60266.0}, 'offset_large_D2': {0: -53118.0, 1: -56843.0, 2: -59869.0}}

@click.command()
@click.option("-f", "--filter", "filter_", is_flag=True,
    help="Perform filtering (no outpout).")
@click.option("-v", "--verbose", is_flag=True,
    help="Print each file examined.")
@click.argument("files", nargs=-1, type=click.Path())
def cmd(filter_, verbose, files):
    t0 = time.time()
    if filter_:
        params = pandas.DataFrame.from_dict(filter_params)
    for f in files:
        try:
            sfile = sfp.seaflowfile.SeaFlowFile(f)
        except sfp.errors.FileError:
            continue
        try:
            evt_df = sfp.fileio.read_evt_labview(f)["df"]
            msg = f"{os.path.basename(f)} {len(evt_df.index)}"
            if filter_:
                evt_df = sfp.particleops.mark_focused(evt_df, params)
                opp_df = sfp.particleops.select_focused(evt_df)
                msg += f" {len(opp_df.index)}"
            if verbose:
                print(msg)
        except sfp.errors.FileError:
            pass
    print("{}".format(time.time() - t0))

if __name__ == "__main__":
    cmd()
