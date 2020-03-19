import os

import numpy as np
import pytest
import seaflowpy as sfp


@pytest.fixture()
def tmpout(tmpdir):
    """Setup to test sampling workflow"""
    evtpaths = [
        "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00",
        "tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz",
    ]
    file_ids = [
        "2014_185/2014-07-04T00-00-02+00-00",
        "2014_185/2014-07-04T00-03-02+00-00",
    ]
    return {
        "evtpaths": evtpaths,
        "file_ids": file_ids,
        "tmpdir": str(tmpdir),
    }


class TestSample:
    def test_sample_evt_single(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "test.gz")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"], 20000, outpath, noise_filter=False, seed=12345
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [40000, 40000]
        assert [r["events_postsampling"] for r in results] == [10000, 10000]
        df = sfp.fileio.read_evt_labview(outpath)
        assert len(df.index) == 20000
        assert len(df[(df["D1"] == 0) & (df["D2"] == 0) & (df["fsc_small"] == 0)]) > 0

    def test_sample_evt_single_noise_filter(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "test.gz")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"], 20000, outpath, noise_filter=True, seed=12345
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [39928, 39925]
        assert [r["events_postsampling"] for r in results] == [10000, 10000]
        df = sfp.fileio.read_evt_labview(outpath)
        assert len(df.index) == 20000
        assert len(df[(df["D1"] == 0) & (df["D2"] == 0) & (df["fsc_small"] == 0)]) == 0

    def test_sample_evt_single_min_filter(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "test.gz")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            min_fsc=1,
            min_chl=25000,
            min_pe=25000,
            seed=12345,
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [379, 471]
        assert [r["events_postsampling"] for r in results] == [379, 471]
        df = sfp.fileio.read_evt_labview(outpath)
        assert len(df.index) == 850
        assert np.min(df["fsc_small"]) >= 1
        assert np.min(df["pe"]) >= 25000
        assert np.min(df["chl_small"]) >= 25000

    def test_sample_evt_single_empty(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "test.gz")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            min_fsc=60000,
            min_chl=60000,
            min_pe=60000,
            seed=12345,
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [0, 0]
        assert [r["events_postsampling"] for r in results] == [0, 0]
        with pytest.raises(sfp.errors.FileError):
            _df = sfp.fileio.read_evt_labview(outpath)

    def test_sample_evt_multi(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "testdir")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"], 20000, outpath, multi=True, seed=12345
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [40000, 40000]
        assert [r["events_postsampling"] for r in results] == [20000, 20000]

        for fid in tmpout["file_ids"]:
            path = os.path.join(outpath, fid + ".gz")
            df = sfp.fileio.read_evt_labview(path)
            assert len(df.index) == 20000
            assert (
                len(df[(df["D1"] == 0) & (df["D2"] == 0) & (df["fsc_small"] == 0)]) > 0
            )

    def test_sample_evt_multi_noise_filter(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "testdir")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            multi=True,
            noise_filter=True,
            seed=12345,
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [39928, 39925]
        assert [r["events_postsampling"] for r in results] == [20000, 20000]

        for fid in tmpout["file_ids"]:
            path = os.path.join(outpath, fid + ".gz")
            df = sfp.fileio.read_evt_labview(path)
            assert len(df.index) == 20000
            assert (
                len(df[(df["D1"] == 0) & (df["D2"] == 0) & (df["fsc_small"] == 0)]) == 0
            )

    def test_sample_evt_multi_min_filter(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "testdir")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            min_fsc=1,
            min_chl=25000,
            min_pe=25000,
            multi=True,
            seed=12345,
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == [
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014_185/2014-07-04T00-03-02+00-00",
        ]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [379, 471]
        assert [r["events_postsampling"] for r in results] == [379, 471]
        for fid, events in zip(tmpout["file_ids"], [379, 471]):
            path = os.path.join(outpath, fid + ".gz")
            df = sfp.fileio.read_evt_labview(path)
            assert len(df.index) == events
            assert np.min(df["fsc_small"]) >= 1
            assert np.min(df["pe"]) >= 25000
            assert np.min(df["chl_small"]) >= 25000

    def test_sample_evt_multi_empty(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "testdir")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            min_fsc=60000,
            min_chl=60000,
            min_pe=60000,
            multi=True,
            seed=12345,
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == [
            "2014_185/2014-07-04T00-00-02+00-00",
            "2014_185/2014-07-04T00-03-02+00-00",
        ]
        assert [r["events"] for r in results] == [40000, 40000]
        assert [r["events_postfilter"] for r in results] == [0, 0]
        assert [r["events_postsampling"] for r in results] == [0, 0]
        for fid in tmpout["file_ids"]:
            path = os.path.join(outpath, fid + ".gz")
            with pytest.raises(sfp.errors.FileError):
                _df = sfp.fileio.read_evt_labview(path)
