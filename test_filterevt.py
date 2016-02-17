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
    def test_get_paths_new_style(self):
        evt = filterevt.EVT("2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"
        assert evt.get_db_file_name() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014_185/2014-07-04T00-00-02+00-00"
        assert evt.get_db_file_name() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"
        assert evt.get_db_file_name() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014_185/2014-07-04T00-00-02+00-00"
        assert evt.get_db_file_name() == "2014-07-04T00-00-02+00-00"

        evt = filterevt.EVT("foo/bar/2014-07-04T00-00-02+00-00", read_data=False)
        assert evt.get_julian_path() == "2014-07-04T00-00-02+00-00"
        assert evt.get_db_file_name() == "2014-07-04T00-00-02+00-00"

    def test_is_evt(self):
        files = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "not_evt_file",
            "testcruise/2014_185/100.evt",
            "testcruise/2014_185/200.evt.gz",
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014-07-04T00-00-02+00-00"
        ]
        results = [filterevt.EVT.is_evt(f) for f in files]
        answers = [True, True, False, True, True, True, True]
        npt.assert_array_equal(results, answers)

    def test_get_paths_old_style(self):
        evt = filterevt.EVT("42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"
        with pytest.raises(filterevt.EVTFileError):
            evt.get_db_file_name()

        evt = filterevt.EVT("2014_185/42.evt", read_data=False)
        assert evt.get_julian_path() == "2014_185/42.evt"
        assert evt.get_db_file_name() == "2014_185/42.evt"

        evt = filterevt.EVT("foo/42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"
        with pytest.raises(filterevt.EVTFileError):
            evt.get_db_file_name()

        evt = filterevt.EVT("foo/2014_185/42.evt", read_data=False)
        assert evt.get_julian_path() == "2014_185/42.evt"
        assert evt.get_db_file_name() == "2014_185/42.evt"

        evt = filterevt.EVT("foo/bar/42.evt", read_data=False)
        assert evt.get_julian_path() == "42.evt"
        with pytest.raises(filterevt.EVTFileError):
            evt.get_db_file_name()

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
        txt = "testcruise|2014-07-04T00-00-02+00-00|1|0|9783|2.30305700391851|1.30423678597587|5.91025807888534|58.4120408652103|2.01861599856769|1.20790074742528|7.622597117578|53.5020800111923"
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


class TestOutput:
    def test_sqlite3_opp_transformed_against_create_opp_for_db(self, tmpout):
        evt = tmpout["evt"]
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", tmpout["db"], transform=True,
                           no_opp=False)
        con = sqlite3.connect(tmpout["db"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
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
        files = filterevt.get_s3_files("testcruise")

        filterevt.filter_files(files=files, cpus=2, cruise="testcruise",
            db=tmpout["db"], s3=True)

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

    @scope1_local
    def test_SCOPE_1_first_19_local(self, tmpout):
        files = filterevt.find_evt_files("SCOPE_1")
        filterevt.filter_files(files=files[:19], cpus=2, cruise="SCOPE_1", db=tmpout["db"])
        cmd = "sqlite3 %s 'SELECT * FROM opp ORDER BY file' | openssl md5" % tmpout["db"]
        md5_opp = check_output(cmd, shell=True).split(None)[-1]
        cmd = "sqlite3 %s 'SELECT * FROM filter ORDER BY file' | openssl md5" % tmpout["db"]
        md5_filter = check_output(cmd, shell=True).split(None)[-1]

        popcycle_md5_opp = "b82e76165424511ed304f5d7afaf0592"
        popcycle_md5_filter = "e49953d3fe43463a1e5a6c84b0ad95f3"

        assert md5_opp == popcycle_md5_opp
        assert md5_filter == popcycle_md5_filter
