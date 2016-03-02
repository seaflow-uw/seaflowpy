import filterevt
import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest
import sqlite3
from subprocess import check_output


scope1_local = pytest.mark.skipif(
    not pytest.config.getoption("--scope1_local"),
    reason="need --scope1_local option to run"
)
s3 = pytest.mark.skipif(
    not pytest.config.getoption("--s3"),
    reason="need --s3 option to run"
)

@pytest.fixture()
def evt():
    return filterevt.EVT("testcruise/2014_185/2014-07-04T00-00-02+00-00")

@pytest.fixture()
def tmpout(tmpdir, evt):
    db = str(tmpdir.join("test.db"))
    filterevt.ensure_tables(db)
    return {
        "db": db,
        "evt": evt,
        "opp": tmpdir.join(evt.path + ".opp").basename,
        "oppdir": tmpdir.join("oppdir")
    }


class TestOpen:
    def test_read_valid_evt(self):
        evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-00-02+00-00")
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "testcruise/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.evt_transformed == False

    def test_read_valid_evt_and_transform(self):
        evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-00-02+00-00",
                            transform=True)
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "testcruise/2014_185/2014-07-04T00-00-02+00-00"
        assert evt.evt_transformed == True

    def test_read_valid_gz_evt(self):
        evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-03-02+00-00.gz")
        assert evt.headercnt == 40000
        assert evt.evt_count == 40000
        assert evt.path == "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz"

    def test_read_empty_evt(self):
        with pytest.raises(filterevt.EVTFileError):
            evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_bad_header_count_evt(self):
        with pytest.raises(filterevt.EVTFileError):
            evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_short_header_evt(self):
        with pytest.raises(filterevt.EVTFileError):
            evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-12-02+00-00")

class TestPathFilenameParsing:
    def test_is_evt(self):
        files = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "not_evt_file",
            "x.evt",
            "testcruise/2014_185/100_1.evt",
            "2014-07-0400-00-02+00-00",
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/200.evt.gz",
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014-07-04T00-00-02+00-00"
        ]
        results = [filterevt.EVT.is_evt(f) for f in files]
        answers = [True, True, False, False, False, False, True, True, True, True]
        assert results == answers

    def test_get_paths_new_style(self):
        evt = filterevt.EVT("2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014_185/2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014_185/2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/bar/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/bar/2014-07-04T00-00-02+00-00.gz", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"

    def test_get_paths_old_style(self):
        evt = filterevt.EVT("42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"

        evt = filterevt.EVT("2014_185/42.evt", read_data=False)
        assert evt.get_julian_path() == "2014_185/42.evt"

        evt = filterevt.EVT("foo/42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"

        evt = filterevt.EVT("foo/2014_185/42.evt", read_data=False)
        assert evt.get_julian_path() == "2014_185/42.evt"

        evt = filterevt.EVT("foo/bar/42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"

        evt = filterevt.EVT("foo/bar/42.evt.gz", read_data=False)
        assert evt.get_julian_path() == "42.evt"

    def test_parse_file_list(self):
        files = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "not_evt_file",
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/200.evt.gz",
        ]
        parsed = filterevt.parse_file_list(files)
        assert parsed == (files[:2] + files[3:])

    def test_find_evt_files(self):
        files = filterevt.find_evt_files("testcruise")
        answer = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/2014-07-04T00-06-02+00-00",
            "testcruise/2014_185/2014-07-04T00-09-02+00-00",
            "testcruise/2014_185/2014-07-04T00-12-02+00-00"
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
        assert evt.opp_count == 345
        assert evt.width == 0.5
        assert evt.offset == 0.0
        assert evt.origin == -1792
        npt.assert_almost_equal(evt.notch1, 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(evt.notch2, 0.7603813559322033510668, decimal=22)

    def test_filter_with_set_params(self, evt):
        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.opp_count == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_almost_equal(evt.notch1, 1.5, decimal=22)
        npt.assert_almost_equal(evt.notch2, 1.1, decimal=22)

    def test_filter_twice_overwrites_old_results(self, evt):
        evt.filter()
        assert evt.opp_count == 345
        assert evt.width == 0.5
        assert evt.offset == 0.0
        assert evt.origin == -1792
        npt.assert_almost_equal(evt.notch1, 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(evt.notch2, 0.7603813559322033510668, decimal=22)

        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.opp_count == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_almost_equal(evt.notch1, 1.5, decimal=22)
        npt.assert_almost_equal(evt.notch2, 1.1, decimal=22)

    def test_stats(self, evt):
        evt.filter()
        evt_stats = evt.calc_evt_stats()
        opp_stats = evt.calc_opp_stats()
        evt_answer = {
            'D1': {'max': 2963.4783432410418, 'mean': 73.08622879073846, 'min': 1.0},
            'D2': {'max': 3156.0618662237976, 'mean': 79.54512712808497, 'min': 1.0},
            'chl_big': {'max': 53.64042220069004, 'mean': 53.45042450208166, 'min': 53.357532766135428},
            'chl_small': {'max': 2696.4112647346287, 'mean': 4.369003421372767, 'min': 1.1734940610013915},
            'fsc_big': {'max': 4.1313203017805806, 'mean': 2.2051450385182534, 'min': 1.2319020328264516},
            'fsc_perp': {'max': 58.68563380375862, 'mean': 58.41150116848148, 'min': 58.29722577023179},
            'fsc_small': {'max': 1652.0286629483903, 'mean': 2.8114101351566445, 'min': 1.0},
            'pe': {'max': 1779.1855898687629, 'mean': 8.329553221519069, 'min': 1.0}
        }
        opp_answer = {
            'D1': {'max': 2946.0375718383511, 'mean': 27.57584311733493, 'min': 1.0755143540156189},
            'D2': {'max': 3156.0618662237976, 'mean': 27.027957147404216, 'min': 1.0},
            'chl_big': {'max': 53.64042220069004, 'mean': 53.46595431797728, 'min': 53.357532766135428},
            'chl_small': {'max': 922.19096252450186, 'mean': 22.266367429741198, 'min': 1.3345760374616036},
            'fsc_big': {'max': 3.380108678220699, 'mean': 2.2238485381723674, 'min': 1.4249794251756174},
            'fsc_perp': {'max': 58.642349877876896, 'mean': 58.42582313084556, 'min': 58.340254959965897},
            'fsc_small': {'max': 1166.1984528866317, 'mean': 23.187903329680267, 'min': 1.1612919251372618},
            'pe': {'max': 1269.1578052463431, 'mean': 74.84328753100617, 'min': 1.0}
        }
        assert evt_stats == evt_answer
        assert opp_stats == opp_answer


class TestTransform:
    def test_transform_one_value(self):
        npt.assert_almost_equal(filterevt.EVT.transform(56173.714285714275),
            1000.0, decimal=10)

    def test_transform_inplace(self, evt):
        orig_df = evt.evt.copy()
        orig_df_copy = orig_df.copy()
        t_df = evt.transform_particles(orig_df, inplace=True)
        # Returned the same object
        assert orig_df is t_df
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df_copy.as_matrix(), t_df.as_matrix())

        orig_df = evt.evt.copy()
        assert evt.evt_transformed == False
        t_df = evt.transform_evt()
        assert evt.evt_transformed == True
        # Returned the same object
        assert t_df is evt.evt
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df.as_matrix(), evt.evt.as_matrix())

        evt.filter()
        orig_df = evt.opp.copy()
        assert evt.opp_transformed == False
        t_df = evt.transform_opp()
        assert evt.opp_transformed == True
        # Returned the same object
        assert t_df is evt.opp
        # Transformation happened in place
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df.as_matrix(), evt.opp.as_matrix())

    def test_transform_not_inplace(self, evt):
        orig_df = evt.evt.copy()
        t_evt = evt.transform_particles(evt.evt, inplace=False)
        # Returned a copy
        assert not t_evt is evt.evt
        # The input is unchanged
        npt.assert_array_equal(orig_df.as_matrix(), evt.evt.as_matrix())
        # The returned copy is different from original version of input
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df.as_matrix(), t_evt.as_matrix())


class TestOutput:
    def test_sqlite3_opp_counts_and_params(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        assert "testcruise" == sqlitedf.cruise[0]
        assert evt.get_julian_path() == sqlitedf.file[0]
        npt.assert_array_equal(
            [evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1, evt.notch2,
                evt.offset, evt.origin, evt.width],
            sqlitedf[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                "notch2", "offset", "origin", "width"]].as_matrix()[0]
        )

    def test_sqlite3_opp(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        stats = evt.calc_opp_stats()
        evt.save_opp_to_db("testcruise", tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        row = sqlitedf.iloc[0]
        for channel in stats:
            if channel == "D1" or channel == "D2":
                continue
            for stat in ["min", "max", "mean"]:
                k = channel + "_" + stat
                assert stats[channel][stat] == row[k]

    def test_binary_opp(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.write_opp_binary(tmpout["opp"])
        opp = filterevt.EVT(tmpout["opp"])
        # Make sure OPP binary file written can be read back as EVT and is
        # exactly the same
        npt.assert_array_equal(evt.opp, opp.evt)

    def test_binary_opp_after_calc_stats(self, tmpout):
        evt = tmpout["evt"]
        # This shouldn't modify evt
        _ = evt.calc_evt_stats()
        evt.filter(offset=0.0, width=0.5)
        # This shouldn't modify opp
        _ = evt.calc_opp_stats()
        evt.write_opp_binary(tmpout["opp"])
        opp = filterevt.EVT(tmpout["opp"])
        # Make sure OPP binary file written can be read back as EVT and is
        # exactly the same
        npt.assert_array_equal(evt.opp, opp.evt)

class TestMultiFileFilter:
    def test_multi_file_filter_local(self, tmpout):
        files = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/2014-07-04T00-06-02+00-00",
            "testcruise/2014_185/2014-07-04T00-09-02+00-00",
            "testcruise/2014_185/2014-07-04T00-12-02+00-00"
        ]

        filterevt.filter_files(files=files, cpus=2, cruise="testcruise",
            db=tmpout["db"], opp_dir=str(tmpout["oppdir"]))

        evts = [filterevt.EVT(files[0]), filterevt.EVT(files[1])]
        for evt in evts:
            evt.filter()
            evt.calc_opp_stats()

        opps = [
            filterevt.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-00-02+00-00"))),
            filterevt.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-03-02+00-00")))
        ]

        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp ORDER BY file", con)

        for i in [0, 1]:
            evt = evts[i]
            row = sqlitedf.iloc[i]
            for channel in evt.stats:
                for stat in ["min", "max", "mean"]:
                    k = channel + "_" + stat
                    assert evt.transform(evt.stats[channel][stat]) == row[k]
            npt.assert_array_equal(
                [
                    evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1,
                    evt.notch2, evt.offset, evt.origin, evt.width
                ],
                row[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                    "notch2", "offset", "origin", "width"]].as_matrix()
            )
            npt.assert_array_equal(evts[i].opp.as_matrix(),
                                   opps[i].evt.as_matrix())

    @s3
    def test_multi_file_filter_S3(self, tmpout):
        files = filterevt.get_s3_files("testcruise", filterevt.SEAFLOW_BUCKET)

        filterevt.filter_files(files=files, cpus=2, cruise="testcruise",
            db=tmpout["db"], opp_dir=str(tmpout["oppdir"]), s3=True,
            s3_bucket=filterevt.SEAFLOW_BUCKET)

        evts = [filterevt.EVT(files[0]), filterevt.EVT(files[1])]
        for evt in evts:
            evt.filter()
            evt.calc_opp_stats()

        opps = [
            filterevt.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-00-02+00-00"))),
            filterevt.EVT(str(tmpout["oppdir"].join("2014_185/2014-07-04T00-03-02+00-00")))
        ]

        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp ORDER BY file", con)

        for i in [0, 1]:
            evt = evts[i]
            row = sqlitedf.iloc[i]
            for channel in evt.stats:
                for stat in ["min", "max", "mean"]:
                    k = channel + "_" + stat
                    assert evt.transform(evt.stats[channel][stat]) == row[k]
            npt.assert_array_equal(
                [
                    evt.opp_count, evt.evt_count, evt.opp_evt_ratio, evt.notch1,
                    evt.notch2, evt.offset, evt.origin, evt.width
                ],
                row[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                    "notch2", "offset", "origin", "width"]].as_matrix()
            )
            npt.assert_array_equal(evts[i].opp.as_matrix(),
                                   opps[i].evt.as_matrix())

    @scope1_local
    def test_SCOPE_1_first_19_local_against_popcycle(self, tmpout):
        files = filterevt.find_evt_files("SCOPE_1")
        filterevt.filter_files(files=files[:19], cpus=2, cruise="SCOPE_1",
                               db=tmpout["db"], opp_dir=str(tmpout["oppdir"]))
        con = sqlite3.connect(tmpout["db"])
        filter_python = pd.read_sql("SELECT * FROM opp ORDER BY file", con)
        con.close()

        # This db was created with the R script test_filter_SCOPE_1.R using
        # popcycle revision deda9a8.
        con = sqlite3.connect("./popcycle-SCOPE_1-first20.db")
        opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file, particle", con)
        filter_R = pd.read_sql("SELECT * FROM filter ORDER BY file", con)
        con.close()

        opps = []
        ints = filterevt.EVT.int_cols
        floats = filterevt.EVT.float_cols
        for f in tmpout["oppdir"].visit(fil=lambda x: str(x).endswith("+00-00")):
            opp = filterevt.EVT(str(f))
            # Make OPP evt dataframe look like dataframe that popcycle creates
            # without file and cruise columns
            opp.evt[ints] = opp.evt[ints].astype(np.int64)
            opp.evt[floats] = filterevt.EVT.transform(opp.evt[floats])
            opp.evt.insert(0, "particle", np.arange(1, opp.evt_count+1, dtype=np.int64))
            opps.append(opp)
        opp_python = pd.concat([o.evt for o in opps])
        npt.assert_array_almost_equal(
            opp_python.as_matrix(),
            opp_R.drop(["cruise", "file"], axis=1).as_matrix(),
            decimal=12
        )

        npt.assert_array_equal(
            filter_python[["cruise", "file"]],
            filter_R[["cruise", "file"]])
        # Compare whole arrays to two decimal places because R code rounds
        # notch1 and notch2 to two decimal places.
        filter_python = filter_python.drop(["cruise", "file"], axis=1)
        filter_python = filter_python[["opp_count", "evt_count",
            "opp_evt_ratio", "notch1", "notch2", "offset", "origin",
            "width"]]
        npt.assert_array_almost_equal(
            filter_python.as_matrix(),
            filter_R.drop(["cruise", "file"], axis=1).as_matrix(),
            decimal=2)
        # R code saves opp_evt_ratio with full precision so compare these
        # these columns with no decimal precision setting.
        npt.assert_array_equal(
            filter_python["opp_evt_ratio"].as_matrix(),
            filter_R["opp_evt_ratio"].as_matrix())
