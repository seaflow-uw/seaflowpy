import numpy.testing as npt
import os
import pandas as pd
import pytest
import shutil
import sqlite3
import subprocess
from .context import seaflowpy as sfp

popcycle = pytest.mark.skipif(
    not pytest.config.getoption("--popcycle"),
    reason="need --popcycle option to run"
)

@popcycle
def test_against_popcycle(tmpdir):
    # Generate popcycle results
    popcycledir = tmpdir.join("popcycle")
    popcycle_cmd = "Rscript tests/generate_popcycle_results.R tests {}".format(str(popcycledir))
    subprocess.check_call(popcycle_cmd.split())

    # Generate seaflowpy results
    dbfile = str(tmpdir.join("testcruise.db"))
    shutil.copyfile("tests/testcruise_paramsonly.db", dbfile)
    os.chmod(dbfile, 0664)  # make the db writeable
    gatingdf = sfp.db.get_gating_table(dbfile)
    gating_id = gatingdf["id"].values[0]
    evt_files = sfp.evt.find_evt_files("tests/testcruise")
    filt_opts = {
        "notch1": None, "notch2": None, "offset": 0.0, "origin": None,
        "width": 0.5
    }
    sfp.filterevt.filter_evt_files(
        evt_files, "testcruise", filt_opts, dbfile, str(tmpdir.join("opp")),
        multiprocessing_flag=False)
    opp_files = sfp.evt.find_evt_files(str(tmpdir.join("opp")))
    sfp.classifyopp.classify_opp_files(
        opp_files, "testcruise", gating_id, dbfile,
        str(tmpdir.join("vct")), multiprocessing_flag=False)

    # Compare opp/vct table output
    with sqlite3.connect(dbfile) as con_py:
        opp_py = pd.read_sql("SELECT * FROM opp ORDER BY file", con_py)
        vct_py = pd.read_sql("SELECT * FROM vct ORDER BY file, pop", con_py)
    with sqlite3.connect(str(popcycledir.join("testcruise.db"))) as con_R:
        opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file", con_R)
        vct_R = pd.read_sql("SELECT * FROM vct ORDER BY file, pop", con_R)

    columns = ["opp_count", "evt_count", "opp_evt_ratio", "notch1", "notch2", "offset", "origin", "width"]
    npt.assert_allclose(opp_py[columns], opp_R[columns])
    npt.assert_allclose(vct_py.groupby("pop").sum(), vct_R.groupby("pop").sum())
    assert "\n".join(opp_py["file"].values) == "\n".join(opp_R["file"].values)

    # Compare OPP file output
    opps_py = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(tmpdir.join("opp")))]
    opps_R = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(popcycledir.join("opp")))]
    assert len(opps_py) == len(opps_R)
    for i in range(len(opps_py)):
        npt.assert_array_equal(opps_py[i].evt, opps_R[i].evt)

    # Compare VCT file output
    vcts_py = [sfp.vct.VCT(v) for v in sfp.vct.find_vct_files(str(tmpdir.join("vct")))]
    vcts_R = [sfp.vct.VCT(v) for v in sfp.vct.find_vct_files(str(popcycledir.join("vct")))]
    assert len(vcts_py) == len(vcts_R)
    vcts_py_str = "\n".join(["\n".join(v.vct["pop"].values) for v in vcts_py])
    vcts_R_str = "\n".join(["\n".join(v.vct["pop"].values) for v in vcts_R])
    assert vcts_py_str == vcts_R_str
