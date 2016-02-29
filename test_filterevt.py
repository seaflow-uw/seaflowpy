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
        assert evt.evtcnt == 40000
        assert evt.path == "testcruise/2014_185/2014-07-04T00-00-02+00-00"

    def test_read_valid_gz_evt(self):
        evt = filterevt.EVT("testcruise/2014_185/2014-07-04T00-03-02+00-00.gz")
        assert evt.headercnt == 40000
        assert evt.evtcnt == 40000
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
        assert evt.oppcnt == 345
        assert evt.width == 0.5
        assert evt.offset == 0.0
        assert evt.origin == -1792
        npt.assert_almost_equal(evt.notch1, 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(evt.notch2, 0.7603813559322033510668, decimal=22)

    def test_filter_with_set_params(self, evt):
        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.oppcnt == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_almost_equal(evt.notch1, 1.5, decimal=22)
        npt.assert_almost_equal(evt.notch2, 1.1, decimal=22)

    def test_filter_twice_overwrites_old_results(self, evt):
        evt.filter()
        assert evt.oppcnt == 345
        assert evt.width == 0.5
        assert evt.offset == 0.0
        assert evt.origin == -1792
        npt.assert_almost_equal(evt.notch1, 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(evt.notch2, 0.7603813559322033510668, decimal=22)

        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        assert evt.oppcnt == 2812
        assert evt.width == 0.75
        assert evt.offset == 100
        assert evt.origin == -1000
        npt.assert_almost_equal(evt.notch1, 1.5, decimal=22)
        npt.assert_almost_equal(evt.notch2, 1.1, decimal=22)

    def test_opp_stats(self, evt):
        evt.filter()
        assert evt.stats == {}
        evt.calc_opp_stats()
        answer = {
            'chl_big': {'max': 32384.0, 'mean': 32357.50434782609, 'min': 32341.0},
            'chl_small': {'max': 55515.0, 'mean': 14797.066666666668, 'min': 2347.0},
            'fsc_big': {'max': 9904.0, 'mean': 6416.139130434783, 'min': 2880.0},
            'fsc_perp': {'max': 33109.0, 'mean': 33078.91594202899, 'min': 33067.0},
            'fsc_small': {'max': 57424.0, 'mean': 14504.811594202898, 'min': 1216.0},
            'pe': {'max': 58112.0, 'mean': 9768.573913043478, 'min': 0.0}
        }
        assert evt.stats == answer


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
            [evt.oppcnt, evt.evtcnt, evt.opp_evt_ratio, evt.notch1, evt.notch2,
                evt.offset, evt.origin, evt.width],
            sqlitedf[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                "notch2", "offset", "origin", "width"]].as_matrix()[0]
        )

    def test_sqlite3_opp_transformed(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        row = sqlitedf.iloc[0]
        for channel in evt.stats:
            for stat in ["min", "max", "mean"]:
                k = channel + "_" + stat
                assert evt.transform(evt.stats[channel][stat]) == row[k]

    def test_sqlite3_opp_not_transformed(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        row = sqlitedf.iloc[0]
        for channel in evt.stats:
            for stat in ["min", "max", "mean"]:
                k = channel + "_" + stat
                assert evt.stats[channel][stat] == row[k]

    def test_binary_opp(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
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
            db=tmpout["db"], binary_dir=str(tmpout["oppdir"]))

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
                    evt.oppcnt, evt.evtcnt, evt.opp_evt_ratio, evt.notch1,
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
            db=tmpout["db"], binary_dir=str(tmpout["oppdir"]), s3=True,
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
                    evt.oppcnt, evt.evtcnt, evt.opp_evt_ratio, evt.notch1,
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
                               db=tmpout["db"], binary_dir=str(tmpout["oppdir"]))
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
            opp.evt.insert(0, "particle", np.arange(1, opp.evtcnt+1, dtype=np.int64))
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
