import gzip
import numpy as np
import numpy.testing as npt
import os
import pandas as pd
import py.path
import pytest
import shutil
import sqlite3
from .context import seaflowpy as sfp
from subprocess import check_output


s3 = pytest.mark.skipif(
    not pytest.config.getoption("--s3"),
    reason="need --s3 option to run"
)


@pytest.fixture()
def evt():
    return sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")


@pytest.fixture()
def tmpout(tmpdir):
    """Setup to test complete filter workflow"""
    db = str(tmpdir.join("test.db"))
    sfp.db.ensure_tables(db)
    return {
        "db": db,
        "oppdir": tmpdir.join("oppdir"),
        "tmpdir": tmpdir
    }


@pytest.fixture()
def tmpout_single(tmpout, evt):
    """Setup to test a single EVT to OPP workflow"""
    evt_path = py.path.local(evt.path)
    tmpout["evt"] = evt
    tmpout["opp_path"] = tmpout["tmpdir"].join(str(evt_path.basename) + ".opp.gz")
    return tmpout


class TestOpen:
    def test_read_valid_evt(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.evt_transformed == False

    def test_read_valid_evt_and_transform(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
                      transform=True)
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.evt_transformed == True

    def test_read_valid_gz_evt(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz")
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz"

    def test_read_valid_evt_subselect_columns(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")
        evtanswer = evt.evt.drop(
            ["time", "pulse_width", "D1", "D2", "fsc_perp", "fsc_big", "chl_big"],
            axis=1
        )
        evtsub = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
                         columns=["fsc_small", "chl_small", "pe"])
        assert evtsub.columns == ["fsc_small", "pe", "chl_small"]
        npt.assert_array_equal(
            evtsub.evt.columns,
            ["fsc_small", "pe", "chl_small"]
        )
        npt.assert_array_equal(evtsub.evt, evtanswer)

    def test_read_valid_evt_subselect_columns_and_transform(self):
        evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
                      transform=True)
        evtanswer = evt.evt.drop(
            ["time", "pulse_width", "D1", "D2", "fsc_perp", "fsc_big", "chl_big"],
            axis=1
        )
        evtsub = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
                         columns=["fsc_small", "chl_small", "pe"],
                         transform=True)
        assert evtsub.columns == ["fsc_small", "pe", "chl_small"]
        npt.assert_array_equal(
            evtsub.evt.columns,
            ["fsc_small", "pe", "chl_small"]
        )
        npt.assert_array_equal(evtsub.evt, evtanswer)

    def test_read_empty_evt(self):
        with pytest.raises(sfp.errors.EVTFileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_bad_header_count_evt(self):
        with pytest.raises(sfp.errors.EVTFileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_short_header_evt(self):
        with pytest.raises(sfp.errors.EVTFileError):
            evt = sfp.EVT("tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00")


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
            "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00"
        ]
        assert files == answer


class TestFilter:
    def test_filter_bad_width(self, evt):
        with pytest.raises(ValueError):
            evt.filter(width=None)

    def test_filter_bad_offset(self, evt):
        with pytest.raises(ValueError):
            evt.filter(offset=None)

    def test_filter_default(self, evt):
        evt.filter()
        assert evt.opp_count == 311
        assert evt.width == 0.5
        assert evt.offset == 0.0
        assert evt.origin == -1792
        npt.assert_almost_equal(evt.notch1, 0.885080147965475, decimal=15)
        npt.assert_almost_equal(evt.notch2, 0.876434676434676, decimal=15)

    def test_filter_with_set_params(self, evt):
        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.opp_count == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_allclose(evt.notch1, 1.5)
        npt.assert_allclose(evt.notch2, 1.1)

    def test_filter_twice_overwrites_old_results(self, evt):
        evt.filter()
        assert evt.width == 0.5
        assert evt.offset == 0.0

        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.opp_count == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_allclose(evt.notch1, 1.5)
        npt.assert_allclose(evt.notch2, 1.1)

    def test_stats(self, evt):
        # Create made up evt and opp data for stats calculations
        evt = sfp.EVT("fake/path", read_data=False)
        evt.evt = pd.DataFrame()
        evt.opp = pd.DataFrame()
        # Make sure evt isn't transformed during stat calculations
        evt.evt_transformed = True
        evt.opp_transformed = True
        i = 0
        for c in evt.columns:
            evt.evt[c] = np.arange(i*100, i*100+10)
            evt.opp[c] = 2 * np.arange(i*100, i*100+10)  # opp will be double evt
            i += 1
        evt.evt_count = 10
        evt.opp_count = 10
        evt_stats = evt.calc_evt_stats()
        opp_stats = evt.calc_opp_stats()
        evt_answer = {
            'D1': {'max': 209.0, 'mean': 204.5, 'min': 200.0},
            'D2': {'max': 309.0, 'mean': 304.5, 'min': 300.0},
            'fsc_small': {'max': 409.0, 'mean': 404.5, 'min': 400.0},
            'fsc_perp': {'max': 509.0, 'mean': 504.5, 'min': 500.0},
            'fsc_big': {'max': 609.0, 'mean': 604.5, 'min': 600.0},
            'pe': {'max': 709.0, 'mean': 704.5, 'min': 700.0},
            'chl_small': {'max': 809.0, 'mean': 804.5, 'min': 800.0},
            'chl_big': {'max': 909.0, 'mean': 904.5, 'min': 900.0}
        }
        opp_answer = {}
        for channel in evt_answer:
            for stat in evt_answer[channel]:
                if not channel in opp_answer:
                    opp_answer[channel] = {}
                opp_answer[channel][stat] = evt_answer[channel][stat] * 2

        # Results can differ in least significant digits depending on how numpy
        # is installed and used (e.g. if you use the Intel Math Library), so
        # convert to an array and compare with set precision.
        def answer2array(answer):
            array = []
            for k in sorted(answer):  # D1, D2, chl_big, etc ...
                array.append([answer[k]["max"], answer[k]["mean"], answer[k]["min"]])
            return array

        npt.assert_allclose(answer2array(evt_stats), answer2array(evt_answer))
        npt.assert_allclose(answer2array(opp_stats), answer2array(opp_answer))

    def test_stats_does_not_modify(self, evt):
        evt.filter()
        orig_evt = evt.evt.copy()
        orig_opp = evt.opp.copy()
        _ = evt.calc_evt_stats()
        _ = evt.calc_opp_stats()
        npt.assert_array_equal(orig_evt, evt.evt)
        npt.assert_array_equal(orig_opp, evt.opp)


class TestTransform:
    def test_transform_one_value(self):
        npt.assert_almost_equal(sfp.EVT.transform(56173.714285714275),
            1000.0, decimal=10)

    def test_transform_inplace(self, evt):
        orig_df = evt.evt.copy()
        orig_df_copy = orig_df.copy()
        t_df = evt.transform_particles(orig_df, inplace=True)
        # Returned the same object
        assert orig_df is t_df
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df_copy, t_df)

        orig_df = evt.evt.copy()
        assert evt.evt_transformed == False
        t_df = evt.transform_evt()
        assert evt.evt_transformed == True
        # Returned the same object
        assert t_df is evt.evt
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, evt.evt)

        evt.filter()
        orig_df = evt.opp.copy()
        assert evt.opp_transformed == False
        t_df = evt.transform_opp()
        assert evt.opp_transformed == True
        # Returned the same object
        assert t_df is evt.opp
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, evt.opp)

    def test_transform_not_inplace(self, evt):
        orig_df = evt.evt.copy()
        t_evt = evt.transform_particles(evt.evt, inplace=False)
        # Returned a copy
        assert t_evt is not evt.evt
        # The input is unchanged
        npt.assert_array_equal(orig_df, evt.evt)
        # The returned copy is different from original version of input
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_evt)


class TestOPPVCT:
    def test_add_vct(self):
        opps = [sfp.EVT(f) for f in sfp.find_evt_files("tests/testcruise_opp")]
        vcts = [sfp.vct.VCT(f) for f in sfp.vct.find_vct_files("tests/testcruise_vct")]

        # By directory
        opps[0].add_vct("tests/testcruise_vct")
        opps[1].add_vct("tests/testcruise_vct")
        assert "\n".join(vcts[0].vct["pop"].values) == "\n".join(opps[0].evt["pop"].values)
        assert "\n".join(vcts[1].vct["pop"].values) == "\n".join(opps[1].evt["pop"].values)

        # By file path
        opps = [sfp.EVT(f) for f in sfp.find_evt_files("tests/testcruise_opp")]
        opps[0].add_vct(os.path.join("tests/testcruise_vct", opps[0].file_id + ".vct.gz"))
        opps[1].add_vct(os.path.join("tests/testcruise_vct", opps[1].file_id + ".vct"))
        assert "\n".join(vcts[0].vct["pop"].values) == "\n".join(opps[0].evt["pop"].values)
        assert "\n".join(vcts[1].vct["pop"].values) == "\n".join(opps[1].evt["pop"].values)


class TestOutput:
    def test_sqlite3_filter_params(self, tmpout):
        opts = {"notch1": None, "notch2": None, "offset": 0.0, "origin": None,
                "width": 0.5}
        filter_id = sfp.db.save_filter_params(tmpout["db"], opts)
        con = sqlite3.connect(tmpout["db"])
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM filter")
        row = dict(cur.fetchone())
        del row["date"]  # Don't test date
        opts["id"] = filter_id
        assert opts == row

    def test_sqlite3_opp_counts_and_params(self, tmpout_single):
        tmpout = tmpout_single
        opts = {"notch1": None, "notch2": None, "offset": 0.0, "origin": None,
                "width": 0.5}
        evt = tmpout["evt"]
        evt.filter(**opts)
        evt.save_opp_to_db("testcruise", "UUID", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        assert "testcruise" == sqlitedf.cruise[0]
        assert evt.file_id == sqlitedf.file[0]
        assert "UUID" == sqlitedf.filter_id[0]
        npt.assert_array_equal(
            [evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1, evt.notch2,
                evt.offset, evt.origin, evt.width],
            sqlitedf[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                "notch2", "offset", "origin", "width"]].as_matrix()[0]
        )

    def test_sqlite3_opp_stats(self, tmpout_single):
        tmpout = tmpout_single
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        stats = evt.calc_opp_stats()
        evt.save_opp_to_db("testcruise", "UUID", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        row = sqlitedf.iloc[0]
        for channel in stats:
            if channel == "D1" or channel == "D2":
                continue
            for stat in ["min", "max", "mean"]:
                k = channel + "_" + stat
                assert stats[channel][stat] == row[k]

    def test_sqlite3_opp_overwrite(self, tmpout_single):
        tmpout = tmpout_single
        opts = {"notch1": None, "notch2": None, "offset": 0.0, "origin": None,
                "width": 0.5}
        evt = tmpout["evt"]
        evt.filter(**opts)
        evt.save_opp_to_db("testcruise", "UUID1", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        assert "UUID1" == sqlitedf.filter_id[0]
        assert len(sqlitedf) == 1

        evt.save_opp_to_db("testcruise", "UUID2", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        # There should only be one filter entry and it should
        # have filter_id == UUID2
        assert "UUID2" == sqlitedf.filter_id[0]
        assert len(sqlitedf) == 1

    def test_binary_opp(self, tmpdir):
        oppdir = tmpdir.join("oppdir")
        popcycle_opp_file = "tests/testcruise_opp/2014_185/2014-07-04T00-00-02+00-00.opp.gz"
        opp = sfp.EVT(popcycle_opp_file)
        opp.opp = opp.evt  # move particle data to opp attribute
        opp.opp_count = len(opp.opp)  # make sure count is updated
        opp.write_opp_binary(str(oppdir))  # output to binary file

        # Make sure OPP binary file written can be read back as EVT and
        # DataFrame is the the same
        reread_opp = sfp.EVT(str(oppdir.join("2014_185/2014-07-04T00-00-02+00-00.opp.gz")))
        npt.assert_array_equal(opp.opp, reread_opp.evt)

        # Check that output opp binary file matches that produced by R code
        # for popcycle 80d17f6
        popcycle_opp = gzip.open(popcycle_opp_file).read()
        new_opp = gzip.open(str(oppdir.join("2014_185/2014-07-04T00-00-02+00-00.opp.gz"))).read()
        assert popcycle_opp == new_opp


class TestMultiFileFilter:
    def test_multi_file_filter_local(self, tmpout):
        """Test multi-file filtering and ensure output can be read back OK"""
        files = [
            "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
            "tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00",
            "tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00"
        ]
        filt_opts = {
            "notch1": None, "notch2": None, "offset": 0.0, "origin": None,
            "width": 0.5
        }

        # python setup.py test doesn't play nice with pytest and
        # multiprocessing, so we set multiprocessing=False here
        sfp.filterevt.filter_evt_files(
            files=files, process_count=1, cruise="testcruise",
            dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            filter_options=filt_opts, multiprocessing_flag=False)

        evts = [sfp.EVT(files[0]), sfp.EVT(files[1])]
        for evt in evts:
            evt.filter()
            evt.stats = evt.calc_opp_stats()

        opps = [
            sfp.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-00-02+00-00.opp.gz"))),
            sfp.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-03-02+00-00.opp.gz")))
        ]

        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp ORDER BY file", con)

        for i in [0, 1]:
            evt = evts[i]
            row = sqlitedf.iloc[i]
            for channel in evt.stats:
                if channel in ["D1", "D2"]:
                    continue
                for stat in ["min", "max", "mean"]:
                    k = channel + "_" + stat
                    assert evt.stats[channel][stat] == row[k]
            npt.assert_array_equal(
                [
                    evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1,
                    evt.notch2, evt.offset, evt.origin, evt.width
                ],
                row[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                    "notch2", "offset", "origin", "width"]].as_matrix()
            )
            npt.assert_array_equal(evts[i].opp, opps[i].evt)

    @s3
    def test_multi_file_filter_S3(self, tmpout):
        """Test S3 multi-file filtering and ensure output can be read back OK"""
        files = sfp.aws.get_s3_files("testcruise", "armbrustlab.seaflow")
        files = sfp.evt.parse_file_list(files)
        filt_opts = {
            "notch1": None, "notch2": None, "offset": 0.0, "origin": None,
            "width": 0.5
        }

        # python setup.py test doesn't play nice with pytest and
        # multiprocessing, so we set multiprocessing=False here
        sfp.filterevt.filter_evt_files(
            files=files, process_count=1, cruise="testcruise",
            dbpath=tmpout["db"], opp_dir=str(tmpout["oppdir"]),
            filter_options=filt_opts, s3=True, s3_bucket="armbrustlab.seaflow",
            multiprocessing_flag=False)

        evts = [
            sfp.EVT(os.path.join("tests", files[0])),
            sfp.EVT(os.path.join("tests", files[1]))
        ]
        for evt in evts:
            evt.filter()
            evt.calc_opp_stats()
            evt.stats = evt.calc_opp_stats()

        opps = [
            sfp.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-00-02+00-00.opp.gz"))),
            sfp.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-03-02+00-00.opp.gz")))
        ]

        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp ORDER BY file", con)

        for i in [0, 1]:
            evt = evts[i]
            row = sqlitedf.iloc[i]
            for channel in evt.stats:
                if channel in ["D1", "D2"]:
                    continue
                for stat in ["min", "max", "mean"]:
                    k = channel + "_" + stat
                    assert evt.stats[channel][stat] == row[k]
            npt.assert_array_equal(
                [
                    evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1,
                    evt.notch2, evt.offset, evt.origin, evt.width
                ],
                row[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                    "notch2", "offset", "origin", "width"]]
            )
            npt.assert_array_equal(evts[i].opp, opps[i].evt)
