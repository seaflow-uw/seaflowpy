import numpy as np
import numpy.testing as npt
import os
import pandas as pd
import py.path
import pytest
import shutil
import sqlite3
from .context import seaflowpy as sfp


# TODO: evt.EVT.test calc_pop_stats()

def test_multi_file_classify(tmpdir):
    dbfile = str(tmpdir.join("testcruise.db"))
    shutil.copyfile("tests/testcruise_paramsonly.db", dbfile)
    os.chmod(dbfile, 0664)  # make the db writeable
    gatingdf = sfp.db.get_gating_table(dbfile)
    gating_id = gatingdf["id"].values[0]
    opp_files = sfp.evt.find_evt_files("tests/testcruise_opp")
    sfp.classifyopp.classify_opp_files(
        opp_files, "testcruise", gating_id, dbfile,
        str(tmpdir.join("vct")), multiprocessing_flag=False)

    # Simple validation of vct table output
    with sqlite3.connect(dbfile) as con:
        sqldf = pd.read_sql("SELECT * FROM vct ORDER BY file, pop", con)

    assert len(sqldf) == 10
    assert len(set(sqldf["file"])) == 2

    # Make sure VCT file count matches DB record counts
    vct_files = sfp.vct.find_vct_files(str(tmpdir.join("vct")))
    assert len(set(sqldf["file"])) == len(vct_files)

    # Make sure population counts are correct
    sums = sqldf.groupby("pop").sum()
    assert sums.loc["beads"]["count"] == 135.0
    assert sums.loc["picoeuks"]["count"] == 36.0
    assert sums.loc["prochloro"]["count"] == 466.0
    assert sums.loc["synecho"]["count"] == 152.0
    assert sums.loc["unknown"]["count"] == 13.0
