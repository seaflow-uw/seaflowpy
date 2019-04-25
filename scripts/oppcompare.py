# Compare multifile opp to single file opp binary files
#
# This script is included in this project to make it possible to validate that
# OPP files with an extra 3-quantile bit flag encoded boolean value contain
# identical data compared to OPP files where quantiles are split into 3
# separate files.
import click
import numpy.testing as npt
import os
import seaflowpy as sfp


@click.command()
@click.argument('old', nargs=1, type=click.Path())
@click.argument('new', nargs=1, type=click.Path())
def cmd(old, new):
    new_dirs = [os.path.join(new, x) for x in os.listdir(new)]
    new_dirs = [x for x in new_dirs if os.path.isdir(x)]
    for d in new_dirs:
        for f in [os.path.join(d, x) for x in os.listdir(d)]:
            sfile = sfp.seaflowfile.SeaFlowFile(f)
            newdf = sfp.fileio.read_opp_labview(f)
            oldfile = os.path.join(old, "2.5", sfile.file_id + ".opp.gz")
            olddf25 = sfp.fileio.read_evt_labview(oldfile)
            oldfile = os.path.join(old, "50", sfile.file_id + ".opp.gz")
            olddf50 = sfp.fileio.read_evt_labview(oldfile)
            oldfile = os.path.join(old, "97.5", sfile.file_id + ".opp.gz")
            olddf975 = sfp.fileio.read_evt_labview(oldfile)

            print(
                sfile.file_id,
                newdf.loc[newdf["q2.5"], "fsc_small"].sum(),
                olddf25["fsc_small"].sum(),
                newdf.loc[newdf["q50"], "fsc_small"].sum(),
                olddf50["fsc_small"].sum(),
                newdf.loc[newdf["q97.5"], "fsc_small"].sum(),
                olddf975["fsc_small"].sum()
            )
            #print(newdf.info())

            #print("old 2.5")
            #print(olddf25.info())
            npt.assert_array_equal(newdf.loc[newdf["q2.5"], sfp.particleops.columns], olddf25)

            #print("old 50")
            #print(olddf50.info())
            npt.assert_array_equal(newdf.loc[newdf["q50"], sfp.particleops.columns], olddf50)

            #print("old 97.5")
            #print(olddf975.info())
            npt.assert_array_equal(newdf.loc[newdf["q97.5"], sfp.particleops.columns], olddf975)



if __name__ == '__main__':
    cmd()
