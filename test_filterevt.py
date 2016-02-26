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
        "opp": tmpdir.join(evt.path + ".opp").basename
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
            "2014-07-0400-00-02+00-00",
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/200.evt.gz",
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014-07-04T00-00-02+00-00"
        ]
        results = [filterevt.EVT.is_evt(f) for f in files]
        answers = [True, True, False, False, False, True, True, True, True]
        npt.assert_array_equal(results, answers)

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
        npt.assert_array_equal(parsed, files[:2] + files[3:])

    def test_find_evt_files(self):
        files = filterevt.find_evt_files("testcruise")
        answer = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/2014-07-04T00-06-02+00-00",
            "testcruise/2014_185/2014-07-04T00-09-02+00-00",
            "testcruise/2014_185/2014-07-04T00-12-02+00-00"
        ]
        npt.assert_array_equal(files, answer)


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

    def test_create_opp_for_db_against_R_popcycle_results(self, evt):
        evt.filter(offset=0.0, width=0.5)
        oppdf = evt.create_opp_for_db("testcruise")
        # Answer from R popcycle sqlite3 database 'select * from opp limit 1'
        txt = "testcruise|2014_185/2014-07-04T00-00-02+00-00|1|0|9783|2.30305700391851|1.30423678597587|5.91025807888534|58.4120408652103|2.01861599856769|1.20790074742528|7.622597117578|53.5020800111923"
        # cruise, file, particle, time, pulse_width, D1, D2, fsc_small, fsc_perp, fsc_big, pe, chl_small, chl_big
        types = [str, str, int, int, int, float, float, float, float, float, float, float, float]
        r_answer = []
        for i, x in enumerate(txt.split("|")):
            r_answer.append(types[i](x))
        opp_str = oppdf.values[0][:2]
        opp_ints = oppdf.values[0][2:5].astype(np.int64)
        opp_floats = oppdf.values[0][5:].astype(np.float64)
        r_str = np.array(r_answer[:2])
        r_ints = np.array(r_answer[2:5], dtype=np.int64)
        r_floats = np.array(r_answer[5:], dtype=np.float64)
        npt.assert_array_equal(opp_str, r_str)
        npt.assert_array_equal(opp_ints, r_ints)
        npt.assert_array_almost_equal(opp_floats, r_floats, decimal=14)

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
            'D1': {'max': 64960.0, 'mean': 10064.834782608696, 'min': 592.0},
            'D2': {'max': 65520.0, 'mean': 8199.420289855072, 'min': 0.0},
            'chl_big': {'max': 32384.0, 'mean': 32357.50434782609, 'min': 32341.0},
            'chl_small': {'max': 55515.0, 'mean': 14797.066666666668, 'min': 2347.0},
            'fsc_big': {'max': 9904.0, 'mean': 6416.139130434783, 'min': 2880.0},
            'fsc_perp': {'max': 33109.0, 'mean': 33078.91594202899, 'min': 33067.0},
            'fsc_small': {'max': 57424.0, 'mean': 14504.811594202898, 'min': 1216.0},
            'pe': {'max': 58112.0, 'mean': 9768.573913043478, 'min': 0.0}
        }
        assert evt.stats == answer


class TestOutput:
    def test_sqlite3_opp_cruise_file(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True,
                           no_opp=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        assert "testcruise" == sqlitedf.cruise[0]
        assert evt.get_julian_path() == sqlitedf.file[0]

    def test_sqlite3_opp_transformed_against_create_opp_for_db(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True,
                           no_opp=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        assert "testcruise" == sqlitedf.cruise[0]
        assert evt.get_julian_path() == sqlitedf.file[0]
        oppdf = evt.create_opp_for_db("testcruise")
        npt.assert_array_equal(oppdf.values, sqlitedf.values)

    def test_sqlite3_opp_not_transformed_against_create_opp_for_db(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=False,
                           no_opp=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        oppdf = evt.create_opp_for_db("testcruise", transform=False)
        npt.assert_array_equal(oppdf.values, sqlitedf.values)

    def test_sqlite3_opp_not_transformed_against_np_array(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=False,
                           no_opp=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        # Strip columns that were added by create_opp_for_db
        sqlitedf = sqlitedf.drop(["cruise", "file", "particle"], axis=1)
        npt.assert_array_equal(evt.opp.values, sqlitedf.values)

    def test_sqlite3_filter_cruise_file(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True,
                           no_opp=True)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM filter", con)
        assert "testcruise" == sqlitedf.cruise[0]
        assert evt.get_julian_path() == sqlitedf.file[0]

    def test_sqlite3_filter(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], no_opp=True)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM filter", con)
        npt.assert_array_equal(
            [evt.oppcnt, evt.evtcnt, evt.opp_evt_ratio, evt.notch1, evt.notch2,
                evt.offset, evt.origin, evt.width],
            sqlitedf[["opp_count", "evt_count", "opp_evt_ratio", "notch1",
                "notch2", "offset", "origin", "width"]].as_matrix()[0]
        )

    def test_sqlite3_filter_transformed(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True,
                           no_opp=True)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM filter", con)
        row = sqlitedf.iloc[0]
        for channel in evt.stats:
            for stat in ["min", "max", "mean"]:
                k = channel + "_" + stat
                assert evt.transform(evt.stats[channel][stat]) == row[k]

    def test_sqlite3_filter_not_transformed(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=False,
                           no_opp=True)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM filter", con)
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

        filterevt.filter_files(files=files, cpus=2, cruise="testcruise", db=tmpout["db"])

        con = sqlite3.connect(tmpout["db"])
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM opp")
        oppcnt = cur.fetchone()[0]
        con.close()
        assert oppcnt == 749

        con = sqlite3.connect(tmpout["db"])
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM filter ORDER BY file")
        rows = cur.fetchall()

        assert rows[0]["opp_count"] == 345
        assert rows[1]["opp_count"] == 404
        assert rows[0]["evt_count"] == 40000
        assert rows[1]["evt_count"] == 40000
        npt.assert_almost_equal(rows[0]["opp_evt_ratio"], 0.008625, decimal=22)
        npt.assert_almost_equal(rows[1]["opp_evt_ratio"], 0.0101, decimal=22)
        npt.assert_almost_equal(rows[0]["notch1"], 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(rows[1]["notch1"], 0.8768736616702355046726, decimal=22)
        npt.assert_almost_equal(rows[0]["notch2"], 0.7603813559322033510668, decimal=22)
        npt.assert_almost_equal(rows[1]["notch2"], 0.8675847457627118286538, decimal=22)
        assert rows[0]["width"] == 0.5
        assert rows[1]["width"] == 0.5
        assert rows[0]["origin"] == -1792
        assert rows[1]["origin"] == -1744
        assert rows[0]["offset"] == 0.0
        assert rows[1]["offset"] == 0.0

    @s3
    def test_multi_file_filter_S3(self, tmpout):
        files = filterevt.get_s3_files("testcruise", filterevt.SEAFLOW_BUCKET)

        filterevt.filter_files(files=files, cpus=2, cruise="testcruise",
            db=tmpout["db"], s3=True, s3_bucket=filterevt.SEAFLOW_BUCKET)

        con = sqlite3.connect(tmpout["db"])
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM opp")
        oppcnt = cur.fetchone()[0]
        con.close()
        assert oppcnt == 749

        con = sqlite3.connect(tmpout["db"])
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM filter ORDER BY file")
        rows = cur.fetchall()
        con.close()

        assert rows[0]["opp_count"] == 345
        assert rows[1]["opp_count"] == 404
        assert rows[0]["evt_count"] == 40000
        assert rows[1]["evt_count"] == 40000
        npt.assert_almost_equal(rows[0]["opp_evt_ratio"], 0.008625, decimal=22)
        npt.assert_almost_equal(rows[1]["opp_evt_ratio"], 0.0101, decimal=22)
        npt.assert_almost_equal(rows[0]["notch1"], 0.7668803418803419313932, decimal=22)
        npt.assert_almost_equal(rows[1]["notch1"], 0.8768736616702355046726, decimal=22)
        npt.assert_almost_equal(rows[0]["notch2"], 0.7603813559322033510668, decimal=22)
        npt.assert_almost_equal(rows[1]["notch2"], 0.8675847457627118286538, decimal=22)
        assert rows[0]["width"] == 0.5
        assert rows[1]["width"] == 0.5
        assert rows[0]["origin"] == -1792
        assert rows[1]["origin"] == -1744
        assert rows[0]["offset"] == 0.0
        assert rows[1]["offset"] == 0.0

    @scope1_local
    def test_SCOPE_1_first_19_local(self, tmpout):
        files = filterevt.find_evt_files("SCOPE_1")
        filterevt.filter_files(files=files[:19], cpus=2, cruise="SCOPE_1", db=tmpout["db"])
        con = sqlite3.connect(tmpout["db"])
        opp_python = pd.read_sql("SELECT * FROM opp ORDER BY file, particle", con)
        filter_python = pd.read_sql("SELECT * FROM filter ORDER BY file", con)
        con.close()

        # This db was created with the R script test_filter_SCOPE_1.R using
        # popcycle revision deda9a8.
        con = sqlite3.connect("./popcycle-SCOPE_1-first20.db")
        opp_R = pd.read_sql("SELECT * FROM opp ORDER BY file, particle", con)
        filter_R = pd.read_sql("SELECT * FROM filter ORDER BY file", con)
        con.close()

        npt.assert_array_equal(
            opp_python[["cruise", "file"]],
            opp_R[["cruise", "file"]])
        npt.assert_array_almost_equal(
            opp_python.drop(["cruise", "file"], axis=1).as_matrix(),
            opp_R.drop(["cruise", "file"], axis=1).as_matrix(),
            decimal=12)

        npt.assert_array_equal(
            filter_python[["cruise", "file"]],
            filter_R[["cruise", "file"]])
        # Compare whole arrays to two decimal places because R code rounds
        # notch1 and notch2 to two decimal places.
        npt.assert_array_almost_equal(
            filter_python.drop(["cruise", "file"], axis=1).as_matrix(),
            filter_R.drop(["cruise", "file"], axis=1).as_matrix(),
            decimal=2)
        # R code saves opp_evt_ratio with full precision so compare these
        # these columns with not decimal precision setting.
        npt.assert_array_equal(
            filter_python["opp_evt_ratio"].as_matrix(),
            filter_R["opp_evt_ratio"].as_matrix())
