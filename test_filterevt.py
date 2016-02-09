import filterevt
import numpy as np
import numpy.testing as npt
import os
import pandas as pd
import mock
import shutil
import sqlite3
import tempfile
import unittest


class OpenTests(unittest.TestCase):
    def setUp(self):
        self.file = "testcruise/2014_185/2014-07-04T00-00-02+00-00"
        self.filegz = "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz"
        self.empty_file = "testcruise/2014_185/2014-07-04T00-06-02+00-00"
        self.bad_header_count_file = "testcruise/2014_185/2014-07-04T00-09-02+00-00"
        self.short_header_file = "testcruise/2014_185/2014-07-04T00-12-02+00-00"

    def test_read_valid_evt(self):
        evt = filterevt.EVT(self.file)
        self.assertEqual(evt.headercnt, 40000)
        self.assertEqual(evt.evtcnt, 40000)
        self.assertEqual(evt.path, self.file)

    def test_read_valid_gz_evt(self):
        evt = filterevt.EVT(self.filegz)
        self.assertEqual(evt.headercnt, 40000)
        self.assertEqual(evt.evtcnt, 40000)
        self.assertEqual(evt.path, self.filegz)

    def test_read_empty_evt(self):
        with self.assertRaises(filterevt.EVTFileError):
            evt = filterevt.EVT(self.empty_file)

    def test_read_bad_header_count_evt(self):
        with self.assertRaises(filterevt.EVTFileError):
            evt = filterevt.EVT(self.bad_header_count_file)

    def test_read_short_header_evt(self):
        with self.assertRaises(filterevt.EVTFileError):
            evt = filterevt.EVT(self.short_header_file)

class PathTests(unittest.TestCase):
    def test_get_paths_new_style(self):
        evt = filterevt.EVT("2014-07-04T00-00-02+00-00", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014-07-04T00-00-02+00-00")
        self.assertEqual(evt.get_db_file_name(), "2014-07-04T00-00-02+00-00")

        evt = filterevt.EVT("2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014_185/2014-07-04T00-00-02+00-00")
        self.assertEqual(evt.get_db_file_name(), "2014-07-04T00-00-02+00-00")

        evt = filterevt.EVT("foo/2014-07-04T00-00-02+00-00", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014-07-04T00-00-02+00-00")
        self.assertEqual(evt.get_db_file_name(), "2014-07-04T00-00-02+00-00")

        evt = filterevt.EVT("foo/2014_185/2014-07-04T00-00-02+00-00", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014_185/2014-07-04T00-00-02+00-00")
        self.assertEqual(evt.get_db_file_name(), "2014-07-04T00-00-02+00-00")

        evt = filterevt.EVT("foo/bar/2014-07-04T00-00-02+00-00", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014-07-04T00-00-02+00-00")
        self.assertEqual(evt.get_db_file_name(), "2014-07-04T00-00-02+00-00")

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
        self.assertSequenceEqual(results, answers)

    def test_get_paths_old_style(self):
        evt = filterevt.EVT("42.evt", read_data=False)
        self.assertEqual(evt.get_julian_path(), "42.evt")
        with self.assertRaises(filterevt.EVTFileError):
            evt.get_db_file_name()

        evt = filterevt.EVT("2014_185/42.evt", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014_185/42.evt")
        self.assertEqual(evt.get_db_file_name(), "2014_185/42.evt")

        evt = filterevt.EVT("foo/42.evt", read_data=False)
        self.assertEqual(evt.get_julian_path(), "42.evt")
        with self.assertRaises(filterevt.EVTFileError):
            evt.get_db_file_name()

        evt = filterevt.EVT("foo/2014_185/42.evt", read_data=False)
        self.assertEqual(evt.get_julian_path(), "2014_185/42.evt")
        self.assertEqual(evt.get_db_file_name(), "2014_185/42.evt")

        evt = filterevt.EVT("foo/bar/42.evt", read_data=False)
        self.assertEqual(evt.get_julian_path(), "42.evt")
        with self.assertRaises(filterevt.EVTFileError):
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
        self.assertSequenceEqual(parsed, files[:2] + files[3:])

    def test_find_evt_files(self):
        files = filterevt.find_evt_files("testcruise")
        answer = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/2014-07-04T00-06-02+00-00",
            "testcruise/2014_185/2014-07-04T00-09-02+00-00",
            "testcruise/2014_185/2014-07-04T00-12-02+00-00"
        ]
        self.assertSequenceEqual(files, answer)


class FilterTests(unittest.TestCase):
    def setUp(self):
        # This file is a valid new style EVT file
        self.file = "testcruise/2014_185/2014-07-04T00-00-02+00-00"
        self.evt = filterevt.EVT(self.file)

    def test_filter_bad_width(self):
        evt = self.evt
        with self.assertRaises(ValueError):
            evt.filter(width=None)

    def test_filter_bad_offset(self):
        evt = self.evt
        with self.assertRaises(ValueError):
            evt.filter(offset=None)

    def test_filter_default(self):
        evt = self.evt
        evt.filter()
        self.assertEqual(evt.oppcnt, 345)
        self.assertEqual(evt.width, 0.5)
        self.assertEqual(evt.offset, 0.0)
        self.assertEqual(evt.origin, -1792)
        self.assertAlmostEqual(evt.notch1, 0.7668803418803419313932, places=22)
        self.assertAlmostEqual(evt.notch2, 0.7603813559322033510668, places=22)

    def test_filter_with_set_params(self):
        evt = self.evt
        evt.filter(offset=100.0, width=0.75, notch1=1.5, notch2=1.1, origin=-1000)
        self.assertEqual(evt.oppcnt, 2812)
        self.assertEqual(evt.width, 0.75)
        self.assertEqual(evt.offset, 100)
        self.assertEqual(evt.origin, -1000)
        self.assertAlmostEqual(evt.notch1, 1.5, places=22)
        self.assertAlmostEqual(evt.notch2, 1.1, places=22)

    def test_create_opp_for_db_against_R_popcycle_results(self):
        evt = self.evt
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


class OutputTests(unittest.TestCase):
    def setUp(self):
        # This file is a valid new style EVT file
        self.file = "testcruise/2014_185/2014-07-04T00-00-02+00-00"
        self.evt = filterevt.EVT(self.file)

        # Get a temp directory
        self.tempdir = tempfile.mkdtemp()
        # Create path for temp sqlite3 file
        self.tempdb = os.path.join(self.tempdir, "test.db")
        # Create path for temp opp binary file
        self.tempopp = os.path.basename(
            os.path.join(self.tempdir, self.file + ".opp"))
        # Create empty popcycle sqlite3 database
        filterevt.ensure_tables(self.tempdb)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_sqlite3_opp_output_transformed_against_create_opp_for_db(self):
        evt = self.evt
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", self.tempdb, transform=True,
                           no_opp=False)
        con = sqlite3.connect(self.tempdb)
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        oppdf = evt.create_opp_for_db("testcruise")
        npt.assert_array_equal(oppdf.values, sqlitedf.values)

    def test_sqlite3_opp_output_not_transformed_against_create_opp_for_db(self):
        evt = self.evt
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", self.tempdb, transform=False,
                           no_opp=False)
        con = sqlite3.connect(self.tempdb)
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        oppdf = evt.create_opp_for_db("testcruise", transform=False)
        npt.assert_array_equal(oppdf.values, sqlitedf.values)

    def test_sqlite3_opp_output_not_transformed_against_np_array(self):
        evt = self.evt
        evt.filter(offset=0.0, width=0.5)
        evt.save_opp_to_db("testcruise", self.tempdb, transform=False,
                           no_opp=False)
        con = sqlite3.connect(self.tempdb)
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)
        # Strip columns that were added by create_opp_for_db
        sqlitedf = sqlitedf.drop(["cruise", "file", "particle"], axis=1)
        npt.assert_array_equal(evt.opp.values, sqlitedf.values)

    def test_binary_opp_output(self):
        evt = self.evt
        evt.filter(offset=0.0, width=0.5)
        evt.write_opp_binary(self.tempopp)
        opp = filterevt.EVT(self.tempopp)
        # Make sure OPP binary file written can be read back as EVT and is
        # exactly the same
        npt.assert_array_equal(evt.opp, opp.evt)


class MultiFileFilterTests(unittest.TestCase):
    def setUp(self):
        # This file is a valid new style EVT file
        self.files = [
            "testcruise/2014_185/2014-07-04T00-00-02+00-00",
            "testcruise/2014_185/2014-07-04T00-03-02+00-00.gz",
            "testcruise/2014_185/2014-07-04T00-06-02+00-00",
            "testcruise/2014_185/2014-07-04T00-09-02+00-00",
            "testcruise/2014_185/2014-07-04T00-12-02+00-00"
        ]

        # Get a temp directory
        self.tempdir = tempfile.mkdtemp()
        # Create path for temp sqlite3 file
        self.tempdb = os.path.join(self.tempdir, "test.db")
        # Create empty popcycle sqlite3 database
        filterevt.ensure_tables(self.tempdb)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_multi_file_filter(self):
        devnull = open(os.devnull, "w")
        with mock.patch("sys.stdout", devnull):
            filterevt.filter_files(files=self.files, cpus=2,
                cruise="testcruise", db=self.tempdb)

        con = sqlite3.connect(self.tempdb)
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM opp")
        oppcnt = cur.fetchone()[0]
        con.close()
        self.assertEqual(oppcnt, 749)

        con = sqlite3.connect(self.tempdb)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT * FROM filter ORDER BY file")
        rows = cur.fetchall()

        self.assertEqual(rows[0]["opp_count"], 345)
        self.assertEqual(rows[1]["opp_count"], 404)
        self.assertEqual(rows[0]["evt_count"], 40000)
        self.assertEqual(rows[1]["evt_count"], 40000)
        self.assertAlmostEqual(rows[0]["opp_evt_ratio"], 0.008625, places=22)
        self.assertAlmostEqual(rows[1]["opp_evt_ratio"], 0.0101, places=22)
        self.assertAlmostEqual(rows[0]["notch1"], 0.7668803418803419313932,
                               places=22)
        self.assertAlmostEqual(rows[1]["notch1"], 0.8768736616702355046726,
                               places=22)
        self.assertAlmostEqual(rows[0]["notch2"], 0.7603813559322033510668,
                               places=22)
        self.assertAlmostEqual(rows[1]["notch2"], 0.8675847457627118286538,
                               places=22)
        self.assertEqual(rows[0]["width"], 0.5)
        self.assertEqual(rows[1]["width"], 0.5)
        self.assertEqual(rows[0]["origin"], -1792)
        self.assertEqual(rows[1]["origin"], -1744)
        self.assertEqual(rows[0]["offset"], 0.0)
        self.assertEqual(rows[1]["offset"], 0.0)



if __name__ == "__main__":
    unittest.main()
