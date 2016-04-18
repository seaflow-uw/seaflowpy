import numpy.testing as npt
import os
import pandas as pd
import py.path
import pytest
from .context import seaflowpy as sfp


class TestOpen:
    def test_read_VCT(self):
        vct = sfp.vct.VCT("tests/testcruise_vct/2014_185/2014-07-04T00-03-02+00-00.vct")
        assert vct.vct_count == 404
        assert vct.path == "tests/testcruise_vct/2014_185/2014-07-04T00-03-02+00-00.vct"
        assert vct.vct_count == len(vct.vct)
        assert vct.vct.columns == ["pop"]

    def test_read_VCT_gz(self):
        vct = sfp.vct.VCT("tests/testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct.gz")
        assert vct.vct_count == 345
        assert vct.path == "tests/testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct.gz"
        assert vct.vct_count == len(vct.vct)
        assert vct.vct.columns == ["pop"]


class TestPathFilenamParsing:
    def test_is_vct(self):
        files = [
            # Valid names
            "testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct",
            "testcruise_vct/2014_185/2014-07-04T00-03-02+00-00.vct.gz",
            "testcruise_vct/2014_185/100.evt.vct",
            "testcruise_vct/2014_185/100.evt.vct.gz",
            "2014_185/2014-07-04T00-00-02+00-00.vct",
            "2014-07-04T00-00-02+00-00.vct",
            "31.evt.opp.31-class.vct",  # support weird old names
            "testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct", # more weird old names

            # Bad names
            "not_vct_file",
            "x.vct",
            "1.vct",
            "1.evt",
            "testcruise_vct/2014_185/100_1.vct",
            "2014-07-0400-00-02+00-00.vct",
            "2014-07-04T00-00-02+00-00.vc",
            "2014_185/2014-07-04T00-00-02+00-00"
        ]
        results = [sfp.vct.is_vct(f) for f in files]
        answers = [
            True, True, True, True, True, True, True, True,
            False, False, False, False, False, False, False, False
        ]
        assert results == answers

    def test_find_vct_file(self):
        files = sfp.vct.find_vct_files("tests/testcruise_vct")
        assert files == [
            "tests/testcruise_vct/2014_185/2014-07-04T00-00-02+00-00.vct.gz",
            "tests/testcruise_vct/2014_185/2014-07-04T00-03-02+00-00.vct"
        ]
