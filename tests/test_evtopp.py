from builtins import str
from builtins import object
import gzip
import hashlib
import io
import numpy as np
import numpy.testing as npt
import os
import pandas as pd
import py.path
import pytest
import shutil
import sqlite3
import subprocess
import seaflowpy as sfp
from subprocess import check_output


popcycle = pytest.mark.skipif(
    not pytest.config.getoption("--popcycle"),
    reason="need --popcycle option to run"
)


s3 = pytest.mark.skipif(
    not pytest.config.getoption("--s3"),
    reason="need --s3 option to run"
)


@pytest.fixture()
def evt():
    return sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")


@pytest.fixture()
def params():
    # Params created with popcycle function
    # create.filter.params(740, 33759, 19543, 19440)
    # taking the 50.0 quantile values.
    return {
        "width": 2500,
        "notch_small_D1": 0.656,
        "notch_small_D2": 0.683,
        "notch_large_D1": 1.635,
        "notch_large_D2": 1.632,
        "offset_small_D1": 0,
        "offset_small_D2": 0,
        "offset_large_D1": -33050,
        "offset_large_D2": -32038
    }


@pytest.fixture()
def tmpout(tmpdir, evt):
    """Setup to test complete filter workflow"""
    # Copy db with filtering params
    db = str(tmpdir.join("testcruise.db"))
    shutil.copyfile("tests/testcruise_paramsonly.db", db)
    os.chmod(db, 0o664)  # make the db writeable
    evt_path = py.path.local(evt.path)
    return {
        "db": db,
        "oppdir": tmpdir.join("oppdir"),
        "opp_path": tmpdir.join(str(evt_path.basename) + ".opp.gz"),
        "tmpdir": tmpdir,
        "evt": evt
    }


class TestOpen(object):
    def test_read_valid_evt(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")
        assert evt.header_count == 40000
        assert evt.event_count == 40000
        assert evt.particle_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.transformed == False

    def test_read_valid_evt_and_transform(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
                      transform=True)
        assert evt.header_count == 40000
        assert evt.event_count == 40000
        assert evt.particle_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.transformed == True

    def test_read_valid_gz_evt(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz")
        assert evt.header_count == 40000
        assert evt.event_count == 40000
        assert evt.particle_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz"
        assert evt.transformed == False

    def test_read_empty_evt(self):
        with pytest.raises(sfp.errors.FileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_bad_header_count_evt(self):
        with pytest.raises(sfp.errors.FileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_short_header_evt(self):
        with pytest.raises(sfp.errors.FileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00")

    def test_read_evt_no_read_data(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.header_count == 0
        assert evt.event_count == 0
        assert evt.particle_count == 0
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.transformed == False


class TestPathFilenameParsing(object):
    def test_is_evt(self):
        files = [
            # Valid names
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/100.evt.opp",
            "testcruise/2014_185/100.evt.opp.gz",
            "testcruise/2014_185/200.evt.gz",
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014-07-04T00-00-02+00-00",
            "2014-07-04T00-00-02+00-00.opp",
            "2014-07-04T00-00-02+00-00.evt",

            # Bad names
            "not_evt_file",
            "x.evt",
            "testcruise/2014_185/100_1.evt",
            "2014-07-0400-00-02+00-00",
            "2014-07-04T00-00-02+00-00.op",
            "2014-07-04T00-00-02+00-00.ev"
        ]
        results = [sfp.evt.is_evt(f) for f in files]
        answers = [
            True, True, True, True, True, True, True, True, True, True,
            False, False, False, False, False, False
        ]
        assert results == answers

    def test_parse_file_list(self):
        files = [
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/200.evt.gz",
            "not_evt_file",
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
        ]
        parsed = sfp.evt.parse_file_list(files)
        assert parsed == (files[:2] + files[3:])

    def test_find_evt_files(self):
        files = sfp.find_evt_files("tests/testcruise_evt")
        answer = [
            "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
            "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
            "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz"
        ]
        assert files == answer


class TestFilter(object):
    def test_filter_no_params(self, evt):
        with pytest.raises(ValueError):
            evt.filter(None)

    def test_filter_empty_params(self, evt):
        with pytest.raises(ValueError):
            evt.filter({})

    def test_filter_with_set_params(self, evt, params):
        opp = evt.filter(params)
        assert opp.parent.event_count == 40000
        assert opp.parent.particle_count == 39928
        assert opp.particle_count == 107
        assert opp.filter_params == params
        assert opp.parent == evt

    def test_noise_filter(self, evt):
        """Events with zeroes in all of D1, D2, and fsc_small are noise"""
        # There are events which could be considered noise (no signal in any of
        # D1, D2, or fsc_small
        assert np.any((evt.df["D1"] == 0) & (evt.df["D2"] == 0) & (evt.df["fsc_small"] == 0)) == True

        signal = evt.filter_noise()

        # We made a new dataframe
        assert signal is not evt.df
        # Things actually got removed
        assert len(signal.index) < len(evt.df.index)
        # Correct event count
        assert len(signal.index) == 39928
        assert evt.particle_count == len(signal.index)

        # No events are all zeroes D1, D2, and fsc_small
        assert np.any((signal["D1"] == 0) & (signal["D2"] == 0) & (signal["fsc_small"] == 0)) == False


class TestTransform(object):
    def test_transform_one_value(self):
        npt.assert_almost_equal(sfp.EVT.transform(56173.714285714275),
            1000.0, decimal=10)

    def test_transform(self, evt):
        orig_df = evt.df.copy()
        npt.assert_array_equal(orig_df, evt.df)
        assert evt.transformed == False
        t_df = evt.transform_particles()
        assert evt.transformed == True
        assert t_df is evt.df
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_df)

    def test_transform_copy(self, evt):
        orig_df = evt.df.copy()
        npt.assert_array_equal(orig_df, evt.df)
        assert evt.transformed == False
        t_df = evt.transform_particles(inplace=False)
        assert evt.transformed == False
        assert t_df is not evt.df
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_df)


class TestOutput(object):
    def test_sqlite3_opp_counts_and_params(self, tmpout, params):
        opp = tmpout["evt"].filter(params)
        opp.save_opp_to_db("UUID", 50.0, tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        assert opp.file_id == sqlitedf["file"][0]
        assert "UUID" == sqlitedf["filter_id"][0]
        assert 50.0 == sqlitedf["quantile"][0]
        npt.assert_array_equal(
            [
                opp.particle_count, opp.parent.particle_count,
                opp.parent.event_count, opp.opp_evt_ratio
            ],
            sqlitedf[[
                "opp_count", "evt_count", "all_count", "opp_evt_ratio"
            ]].values[0]
        )

    def test_binary_evt_output(self, tmpdir):
        evtdir = tmpdir.join("evtdir")
        evt_file = "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        evt = sfp.EVT(evt_file)
        evt.write_binary(str(evtdir), opp=False)  # output to binary file

        # Make sure EVT binary file written can be read back as EVT and
        # DataFrame is the the same
        reread_evt = sfp.EVT(str(evtdir.join("2014_185/2014-07-04T00-00-02+00-00.gz")))
        npt.assert_array_equal(evt.df, reread_evt.df)

        # Check that output evt binary file matches input file
        input_evt = io.open(evt_file, "rb").read()
        new_evt = gzip.open(str(evtdir.join("2014_185/2014-07-04T00-00-02+00-00.gz"))).read()
        assert input_evt == new_evt

    def test_binary_opp_output(self, tmpdir):
        oppdir = tmpdir.join("oppdir")
        opp_file = "tests/testcruise_opp/2014_185/2014-07-04T00-00-02+00-00.opp.gz"
        opp = sfp.EVT(opp_file)
        opp.write_binary(str(oppdir))  # output to binary file

        # Make sure OPP binary file written can be read back as EVT and
        # DataFrame is the the same
        reread_opp = sfp.EVT(str(oppdir.join("2014_185/2014-07-04T00-00-02+00-00.opp.gz")))
        npt.assert_array_equal(opp.df, reread_opp.df)

        # Check that output opp binary file matches input file
        input_opp = gzip.open(opp_file).read()
        new_opp = gzip.open(str(oppdir.join("2014_185/2014-07-04T00-00-02+00-00.opp.gz"))).read()
        assert input_opp == new_opp


class TestMultiFileFilter(object):
    def test_multi_file_filter_local(self, tmpout):
        """Test multi-file filtering and ensure output can be read back OK"""
        files = [
            "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",     # normal file
            "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",  # normal file, gz
            "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",     # empty file
            "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",     # truncated after header
            "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",     # file only 2 bytes, should be at least 4 for header
            "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",  # all noise
            "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz"   # only 2 quantiles have OPP
        ]

        # python setup.py test doesn't play nice with pytest and
        # multiprocessing, so we use one core here
        sfp.filterevt.filter_evt_files(
            files=files, process_count=1,
            dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"])
        )

        multi_file_asserts(tmpout)

    @s3
    def test_multi_file_filter_S3(self, tmpout):
        """Test S3 multi-file filtering and ensure output can be read back OK"""
        config = sfp.conf.get_aws_config()
        cloud = sfp.clouds.AWS(config.items("aws"))
        files = cloud.get_files("testcruise_evt")
        files = sfp.evt.parse_file_list(files)

        # python setup.py test doesn't play nice with pytest and
        # multiprocessing, so we use one core here
        sfp.filterevt.filter_evt_files(
            files=files, process_count=1,
            dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            s3=True)

        multi_file_asserts(tmpout)

    @popcycle
    def test_against_popcycle(self, tmpout):
        # Generate popcycle results
        popcycledir = tmpout["tmpdir"].join("popcycle")
        popcycle_cmd = "Rscript tests/generate_popcycle_results.R tests {}".format(str(popcycledir))
        subprocess.check_call(popcycle_cmd.split())

        # Generate seaflowpy results
        files = [
            "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
            "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-15-02+00-00.gz",
            "tests/testcruise_evt/2014_185/2014-07-04T00-17-02+00-00.gz"
        ]
        sfp.filterevt.filter_evt_files(
            files=files, process_count=1,
            dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"])
        )
        opp_files = sfp.evt.find_evt_files(str(tmpout["oppdir"]))

        # Compare opp table output
        with sqlite3.connect(tmpout["db"]) as con_py:
            opp_py = pd.read_sql("SELECT * FROM opp ORDER BY file, quantile", con_py)
        with sqlite3.connect(str(popcycledir.join("testcruise.db"))) as con_R:
            opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file, quantile", con_R)

        columns = ["opp_count", "evt_count", "opp_evt_ratio", "quantile"]
        npt.assert_allclose(opp_py[columns], opp_R[columns])
        assert "\n".join(opp_py["file"].values) == "\n".join(opp_R["file"].values)

        # Compare OPP file output
        opps_py = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(tmpout["oppdir"]))]
        opps_R = [sfp.evt.EVT(o) for o in sfp.evt.find_evt_files(str(popcycledir.join("opp")))]
        assert len(opps_py) == len(opps_R)
        assert len(opps_py) > 0
        assert len(opps_R) > 0
        for i in range(len(opps_py)):
            npt.assert_array_equal(opps_py[i].df, opps_R[i].df)

def multi_file_asserts(tmpout):
    # Check MD5 checksums of uncompressed OPP files
    hashes = {
        "2.5/2014_185/2014-07-04T00-00-02+00-00.opp.gz": "91e6df7f7754ece7e41095e0f6a1e6fd",
        "2.5/2014_185/2014-07-04T00-03-02+00-00.opp.gz": "99f051960d20663fcf51764f45a729b5",
        "50/2014_185/2014-07-04T00-00-02+00-00.opp.gz": "2cc3137a2baca6fb5bc2f6fd878cb154",
        "50/2014_185/2014-07-04T00-03-02+00-00.opp.gz": "efd5c066b3bac65f6f135f021d95a755",
        "97.5/2014_185/2014-07-04T00-00-02+00-00.opp.gz": "2ad5d4850adf6f8641f966684d3f3dee",
        "97.5/2014_185/2014-07-04T00-03-02+00-00.opp.gz": "268b447bd9e0b91411d0b8c68c7813b4"
    }
    for q in ["2.5", "50", "97.5"]:
        for f in ["2014_185/2014-07-04T00-00-02+00-00.opp.gz", "2014_185/2014-07-04T00-03-02+00-00.opp.gz"]:
            f_path = str(tmpout["oppdir"].join("{}/{}".format(q, f)))
            f_md5 = hashlib.md5(gzip.open(f_path).read()).hexdigest()
            assert f_md5 == hashes["{}/{}".format(q, f)]

    # Check numbers stored in opp table are correct
    filter_params = sfp.db.get_latest_filter(tmpout["db"])
    filter_id = filter_params.iloc[0]["id"]
    opp_table = sfp.db.get_opp(tmpout["db"], filter_id)
    npt.assert_array_equal(
        opp_table["all_count"],
        pd.Series([
            40000, 40000, 40000,
            40000, 40000, 40000,
            0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            40000, 40000, 40000,
            40000, 40000, 40000
        ], name="all_count")
    )
    npt.assert_array_equal(
        opp_table["evt_count"],
        pd.Series([
            39928, 39928, 39928,
            39925, 39925, 39925,
            0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            39925, 39925, 39925
        ], name="evt_count")
    )
    npt.assert_array_equal(
        opp_table["opp_count"],
        pd.Series([
            423, 107, 86,
            492, 182, 147,
            0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            0, 0, 0,
            0, 17, 19
        ], name="opp_count")
    )
    npt.assert_array_equal(
        opp_table["opp_evt_ratio"],
        (opp_table["opp_count"] / opp_table["evt_count"]).replace(pd.np.inf, 0).replace(pd.np.NaN, 0)
    )
