import numpy as np
import numpy.testing as npt
import os
import pandas as pd
import py.path
import pytest
import shutil
import sqlite3
from .context import seaflowpy as sfp


"""
Test answers were created wtih popcycle 80d17f6. testcruise_opp files were
used as input. testcruise.db is a read-only copy of the popcycle db results and
testcruise_vct is the VCT file output.
"""


@pytest.fixture()
def tmpout(tmpdir):
    """Setup to test population classifications"""
    db = str(tmpdir.join("test.db"))
    # Copy the popcycle testcruise.db file. We'll use this to get correct
    # gates and to compare against popcycle classification answers.
    shutil.copyfile("tests/testcruise.db", db)
    with sqlite3.connect(db) as con:
        gatingdf = pd.read_sql("SELECT * FROM gating", con)
        gating_id = gatingdf["id"].values[0]

    return {
        "db": db,  # output db
        "origdb": "tests/testcruise.db",  # popcycle answers
        "gating_id": gating_id,
        "vctdir": tmpdir.join("vctdir"),  # output VCT
        "tmpdir": tmpdir
    }


def test_multi_file_classify(tmpout):
    opp_files = sfp.evt.find_evt_files("tests/testcruise_opp")
    sfp.classifyopp.classify_opp_files(
        opp_files, "testcruise_py", tmpout["gating_id"], tmpout["db"],
        str(tmpout["vctdir"]), multiprocessing_flag=False)

    # Compare vct table output
    with sqlite3.connect(tmpout["db"]) as con_py:
        py_results = pd.read_sql("SELECT * FROM vct WHERE cruise = 'testcruise_py' ORDER BY file, pop", con_py)
    with sqlite3.connect(tmpout["origdb"]) as con_R:
        R_results = pd.read_sql("SELECT * FROM vct ORDER BY file, pop", con_R)

    npt.assert_allclose(
        py_results.groupby("pop").sum(),
        R_results.groupby("pop").sum()
    )

    # Compare VCT file output
    py_vcts = [
        sfp.vct.VCT(str(tmpout["vctdir"].join("2014_185/2014-07-04T00-00-02+00-00.vct.gz"))),
        sfp.vct.VCT(str(tmpout["vctdir"].join("2014_185/2014-07-04T00-03-02+00-00.vct.gz")))
    ]
    R_vcts = [
        sfp.vct.VCT("tests/testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct.gz"),
        sfp.vct.VCT("tests/testcruise_vct/2014_185/2014-07-04T00-03-02+00-00.vct")
    ]

    assert "\n".join(py_vcts[0].vct["pop"].values) == "\n".join(R_vcts[0].vct["pop"].values)
    assert "\n".join(py_vcts[1].vct["pop"].values) == "\n".join(R_vcts[1].vct["pop"].values)
