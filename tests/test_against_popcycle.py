from builtins import str
from builtins import range
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
    os.chmod(dbfile, 0o664)  # make the db writeable
    evt_files = sfp.evt.find_evt_files("tests/testcruise_evt")
    filt_opts = {
        "notch1": None, "notch2": None, "offset": 0.0, "origin": None,
        "width": 0.5
    }
    sfp.filterevt.filter_evt_files(
        evt_files, "testcruise", filt_opts, dbfile, str(tmpdir.join("opp")))
    opp_files = sfp.evt.find_evt_files(str(tmpdir.join("opp")))

    # Compare opp/vct table output
    with sqlite3.connect(dbfile) as con_py:
        opp_py = pd.read_sql("SELECT * FROM opp ORDER BY file", con_py)
    with sqlite3.connect(str(popcycledir.join("testcruise.db"))) as con_R:
        opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file", con_R)

    columns = ["opp_count", "evt_count", "opp_evt_ratio", "notch1", "notch2", "offset", "origin", "width"]
    npt.assert_allclose(opp_py[columns], opp_R[columns])
    assert "\n".join(opp_py["file"].values) == "\n".join(opp_R["file"].values)

    # Compare OPP file output
    opps_py = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(tmpdir.join("opp")))]
    opps_R = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(popcycledir.join("opp")))]
    assert len(opps_py) == len(opps_R)
    for i in range(len(opps_py)):
        npt.assert_array_equal(opps_py[i].df, opps_R[i].df)
