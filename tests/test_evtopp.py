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
def evt_df():
    return sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")


@pytest.fixture()
def params():
    # Params created with popcycle function
    # create.filter.params(740, 33759, 19543, 19440)
    # taking the 50.0 quantile values.
    return pd.DataFrame.from_dict({
        "width": [2500, 2500, 2500],
        "notch_small_D1": [0.614, 0.656, 0.698],
        "notch_small_D2": [0.651, 0.683, 0.714],
        "notch_large_D1": [1.183, 1.635, 2.087],
        "notch_large_D2": [1.208, 1.632, 2.056],
        "offset_small_D1": [1418, 0, -1418],
        "offset_small_D2": [1080, 0, -1047],
        "offset_large_D1": [-17791, -33050, -48309],
        "offset_large_D2": [-17724, -32038, -46352],
        "quantile": [2.5, 50.0, 97.5]
    })


@pytest.fixture()
def tmpout(tmpdir):
    """Setup to test complete filter workflow"""
    # Copy db with filtering params
    db = str(tmpdir.join("testcruise.db"))
    shutil.copyfile("tests/testcruise_paramsonly.db", db)
    os.chmod(db, 0o664)  # make the db writeable
    evt_path = py.path.local("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")
    return {
        "db": db,
        "oppdir": str(tmpdir.join("oppdir")),
        "tmpdir": str(tmpdir),
        "evt_df": sfp.fileio.read_evt_labview(str(evt_path)),
        "evt_path": str(evt_path)
    }


class TestOpen:
    def test_read_valid_evt(self):
        df = sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")
        assert len(df.index) == 40000
        assert list(df) == sfp.particleops.columns

    def test_read_valid_gz_evt(self):
        df = sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz")
        assert len(df.index) == 40000
        assert list(df) == sfp.particleops.columns

    def test_read_empty_evt(self):
        with pytest.raises(sfp.errors.FileError):
            df = sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_bad_header_count_evt(self):
        with pytest.raises(sfp.errors.FileError):
            df = sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_short_header_evt(self):
        with pytest.raises(sfp.errors.FileError):
            df = sfp.fileio.read_evt_labview("tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00")


class TestPathFilenameParsing:
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


class TestFilter:
    def test_mark_focused_no_params(self, evt_df):
        with pytest.raises(ValueError):
            df = sfp.particleops.mark_focused(evt_df, None)

    def test_mark_focused_empty_params(self, evt_df):
        with pytest.raises(ValueError):
            df = sfp.particleops.mark_focused(evt_df, pd.DataFrame.from_dict({}))

    def test_mark_focused_with_set_params(self, evt_df, params):
        evt_df = sfp.particleops.mark_focused(evt_df, params)
        assert len(evt_df.index) == 40000
        assert len(evt_df[evt_df["q2.5"]].index) == 423
        assert len(evt_df[evt_df["q50"]].index) == 107
        assert len(evt_df[evt_df["q97.5"]].index) == 86
        assert len(sfp.particleops.select_focused(evt_df).index) == 427

    def test_noise_filter(self, evt_df):
        """Events with zeroes in all of D1, D2, and fsc_small are noise"""
        # There are events which could be considered noise (no signal in any of
        # D1, D2, or fsc_small
        assert np.any((evt_df["D1"] == 0) & (evt_df["D2"] == 0) & (evt_df["fsc_small"] == 0)) == True

        evt_df = sfp.particleops.mark_noise(evt_df)
        signal_df = evt_df[~evt_df["noise"]]

        # Correct event count
        assert len(signal_df.index) == 39928

        # No events are all zeroes D1, D2, and fsc_small
        assert np.any((signal_df["D1"] == 0) & (signal_df["D2"] == 0) & (signal_df["fsc_small"] == 0)) == False


class TestTransform:
    def test_transform_four_values(self):
        input_df = pd.DataFrame({
            "fsc_small": [32768, 65536],
            "D1": [0, 1]
        })
        output_df = pd.DataFrame({
            "fsc_small": [56.234132519, 3162.2776601684],
            "D1": [1, 1.0001229789]
        })
        npt.assert_array_almost_equal(
            sfp.particleops.transform_particles(input_df, columns=["fsc_small", "D1"]),
            output_df
        )

    def test_transform_copy(self, evt_df):
        orig_df = evt_df.copy()
        npt.assert_array_equal(orig_df, evt_df)
        t_df = sfp.particleops.transform_particles(evt_df)
        assert t_df is not evt_df
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_df)


class TestOutput:
    def test_sqlite3_opp_counts_and_params(self, tmpout, params):
        sf_file = sfp.SeaFlowFile(tmpout["evt_path"])
        df = tmpout["evt_df"]
        df = sfp.particleops.mark_focused(df, params)

        raw_count = len(df.index)
        signal_count = len(df[df["noise"] == False].index)

        sfp.db.save_opp_to_db(sf_file.file_id, df, raw_count, signal_count,
            "UUID", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        try:
            opp_evt_ratio = len(df[df["q50"]].index) / len(df[df["noise"] == False].index)
        except ZeroDivisionError:
            opp_evt_ratio = 0.0

        assert sf_file.file_id == sqlitedf["file"][1]
        assert "UUID" == sqlitedf["filter_id"][1]
        assert 50 == sqlitedf["quantile"][1]
        npt.assert_array_equal(
            [ 107, 39928, 40000, opp_evt_ratio ],
            sqlitedf[["opp_count", "evt_count", "all_count", "opp_evt_ratio"]].values[1]
        )


    def test_sqlite3_opp_counts_and_params_empty(self, tmpout, params):
        sf_file = sfp.SeaFlowFile(tmpout["evt_path"])
        df = sfp.particleops.empty_df()
        df = sfp.particleops.mark_focused(df, params)

        raw_count = len(df.index)
        signal_count = len(df[df["noise"] == False].index)

        sfp.db.save_opp_to_db(sf_file.file_id, df, raw_count, signal_count,
            "UUID", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        try:
            opp_evt_ratio = sqlitedf["opp_count"] / sqlitedf["evt_count"]
        except ZeroDivisionError:
            opp_evt_ratio = 0.0

        assert sf_file.file_id == sqlitedf["file"][1]
        assert "UUID" == sqlitedf["filter_id"][1]
        assert 50 == sqlitedf["quantile"][1]
        npt.assert_array_equal(
            [ 0, 0, 0, 0.0 ],
            sqlitedf[["opp_count", "evt_count", "all_count", "opp_evt_ratio"]].values[1]
        )

    def test_binary_evt_output(self, tmpout):
        sfile = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        evtdir = os.path.join(tmpout["tmpdir"], "evtdir")
        evt_df = sfp.fileio.read_evt_labview(tmpout["evt_path"])

        # Output to binary uncompressed file
        sfp.fileio.write_evt_labview(evt_df, sfile.file_id, evtdir, gz=False)
        # Make sure EVT binary file written can be read back as EVT and
        # DataFrame is the the same
        out_evt_path = os.path.join(evtdir, sfile.file_id)
        reread_evt_df = sfp.fileio.read_evt_labview(out_evt_path)
        npt.assert_array_equal(evt_df, reread_evt_df)
        # Check that output evt binary file matches input file
        input_evt = io.open(tmpout["evt_path"], "rb").read()
        new_evt = io.open(out_evt_path, "rb").read()
        assert input_evt == new_evt

    def test_binary_evt_output_gz(self, tmpout):
        sfile = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        evtdir = os.path.join(tmpout["tmpdir"], "evtdir")
        evt_df = sfp.fileio.read_evt_labview(tmpout["evt_path"])

        # Output to gzipped binary file
        sfp.fileio.write_evt_labview(evt_df, sfile.file_id, evtdir)
        # Make sure EVT binary file written can be read back as EVT and
        # DataFrame is the the same
        out_evt_path = os.path.join(evtdir, sfile.file_id + ".gz")
        reread_evt_df = sfp.fileio.read_evt_labview(out_evt_path)
        npt.assert_array_equal(evt_df, reread_evt_df)
        # Check that output evt binary file matches input file
        input_evt = io.open(tmpout["evt_path"], "rb").read()
        new_evt = gzip.open(out_evt_path).read()
        assert input_evt == new_evt

    def test_binary_opp_output_None(self, tmpout):
        sfile = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        oppdir = os.path.join(tmpout["tmpdir"], "oppdir")
        evt_df = None
        out_opp_path = os.path.join(oppdir, sfile.file_id + ".opp.gz")
        sfp.fileio.write_opp_labview(evt_df, sfile.file_id, oppdir)
        assert os.path.exists(out_opp_path) == False

    def test_binary_opp_output(self, tmpout, params):
        sfile = sfp.SeaFlowFile(tmpout["evt_path"])

        df = tmpout["evt_df"]
        df = sfp.particleops.mark_focused(df, params)
        # df should now have 4 new columns for noise and 3 quantiles
        # Write an opp file
        df = sfp.particleops.select_focused(df)
        sfp.fileio.write_opp_labview(df, sfile.file_id, tmpout["oppdir"], gz=False)
        # Read it back
        opp_path = os.path.join(tmpout["oppdir"], sfile.file_id + ".opp")
        reread_opp_df = sfp.fileio.read_opp_labview(opp_path)
        # Should equal original df with only focused particles
        npt.assert_array_equal(
            df,
            reread_opp_df
        )

    def test_binary_opp_output_gz(self, tmpout, params):
        sfile = sfp.SeaFlowFile(tmpout["evt_path"])

        df = tmpout["evt_df"]
        df = sfp.particleops.mark_focused(df, params)
        # df should now have 4 new columns for noise and 3 quantiles
        # Write an opp file
        df = sfp.particleops.select_focused(df)
        sfp.fileio.write_opp_labview(df, sfile.file_id, tmpout["oppdir"])
        # Read it back
        opp_path = os.path.join(tmpout["oppdir"], sfile.file_id + ".opp.gz")
        reread_opp_df = sfp.fileio.read_opp_labview(opp_path)
        # Should equal original df with only focused particles
        npt.assert_array_equal(
            df,
            reread_opp_df
        )



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
            files=files, dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            worker_count=1
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
            files=files, dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            worker_count=1, s3=True
        )

        multi_file_asserts(tmpout)

    @popcycle
    def test_against_popcycle(self, tmpout):
        # Generate popcycle results
        popcycledir = os.path.join(tmpout["tmpdir"], "popcycle")
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
            files=files, dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            worker_count=1
        )
        opp_files = sfp.evt.find_evt_files(str(tmpout["oppdir"]))

        # Compare opp table output
        with sqlite3.connect(tmpout["db"]) as con_py:
            opp_py = pd.read_sql("SELECT * FROM opp ORDER BY file, quantile", con_py)
        with sqlite3.connect(os.path.join(popcycledir, "testcruise.db")) as con_R:
            opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file, quantile", con_R)

        columns = ["opp_count", "evt_count", "opp_evt_ratio", "quantile"]
        npt.assert_allclose(opp_py[columns], opp_R[columns])
        assert "\n".join(opp_py["file"].values) == "\n".join(opp_R["file"].values)

        # Compare OPP file output
        opps_py = [sfp.fileio.read_opp_labview(o) for o in sfp.evt.find_evt_files(tmpout["oppdir"])]
        opps_R = [sfp.fileio.read_opp_labview(o) for o in sfp.evt.find_evt_files(os.path.join(popcycledir, "opp"))]
        assert len(opps_py) == len(opps_R)
        assert len(opps_py) == 2
        assert len(opps_R) == 2
        for i in range(len(opps_py)):
            npt.assert_array_equal(opps_py[i], opps_R[i])

def multi_file_asserts(tmpout):
    # Check MD5 checksums of uncompressed OPP files
    hashes = {
        "2014_185/2014-07-04T00-00-02+00-00.opp.gz": "c2a7bd9ba30e181210ac30ef806715f9",
        "2014_185/2014-07-04T00-03-02+00-00.opp.gz": "b0a2a016cf7e0e00f9eb9e993b5d4b4e"
    }
    for f in ["2014_185/2014-07-04T00-00-02+00-00.opp.gz", "2014_185/2014-07-04T00-03-02+00-00.opp.gz"]:
        f_path = os.path.join(tmpout["oppdir"], f)
        f_md5 = hashlib.md5(gzip.open(f_path).read()).hexdigest()
        assert f_md5 == hashes[f]

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
    npt.assert_array_equal(
        opp_table["file"],
        pd.Series(' '.join([
            "2014_185/2014-07-04T00-00-02+00-00 " * 3,
            "2014_185/2014-07-04T00-03-02+00-00 " * 3,
            "2014_185/2014-07-04T00-06-02+00-00 " * 3,
            "2014_185/2014-07-04T00-09-02+00-00 " * 3,
            "2014_185/2014-07-04T00-12-02+00-00 " * 3,
            "2014_185/2014-07-04T00-15-02+00-00 " * 3,
            "2014_185/2014-07-04T00-17-02+00-00 " * 3
        ]).split(), name="opp_count")
    )
