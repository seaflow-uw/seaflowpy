import filterevt
import os
import unittest


class GoodNewFileTests(unittest.TestCase):
    def setUp(self):
        # This file is a valid new style EVT file
        self.file = "2014_185/2014-07-04T00-00-02+00-00"
        self.evt = filterevt.EVT(self.file)

    def test_read_evt(self):
        evt = self.evt
        self.assertEqual(evt.headercnt, 40000)
        self.assertEqual(evt.evtcnt, 40000)
        self.assertEqual(evt.path, self.file)
        self.assertTrue(evt.ok)

    def test_get_julian_path(self):
        evt = self.evt
        self.assertEqual(
            evt.get_julian_path(),
            self.file)

    def test_get_db_file_name(self):
        evt = self.evt
        self.assertEqual(
            evt.get_db_file_name(),
            os.path.basename(self.file))

    def test_filter(self):
        evt = self.evt
        evt.filter(offset=0.0, width=0.5)
        self.assertEqual(evt.oppcnt, 345)
        self.assertEqual(evt.width, 0.5)
        self.assertEqual(evt.offset, 0.0)
        self.assertEqual(evt.origin, -1792)
        self.assertAlmostEqual(evt.notch1, 0.766880341880342, places=15)
        self.assertAlmostEqual(evt.notch2, 0.760381355932203, places=15)


if __name__ == "__main__":
    unittest.main()
