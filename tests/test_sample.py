import datetime
import os

import numpy as np
import pandas as pd
import pytest
import pytz
import seaflowpy as sfp

# pylint: disable=redefined-outer-name

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
    dates = [
        datetime.datetime.fromisoformat("2014-07-04T00:00:02+00:00"),
        datetime.datetime.fromisoformat("2014-07-04T00:03:02+00:00"),
    ]
    dates = [d.replace(tzinfo=pytz.utc) for d in dates]
    dates_lookup = dict(zip(file_ids, dates))

    return {
        "evtpaths": evtpaths,
        "file_ids": file_ids,
        "tmpdir": str(tmpdir),
        "dates": dates_lookup
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
        df = pd.read_parquet(outpath)
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
        df = pd.read_parquet(outpath)
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
        df = pd.read_parquet(outpath)
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
        df = pd.read_parquet(outpath)
        assert len(df.index) == 0

    def test_sample_evt_single_dates(self, tmpout):
        outpath = os.path.join(tmpout["tmpdir"], "test.gz")
        results, errs = sfp.sample.sample(
            tmpout["evtpaths"],
            20000,
            outpath,
            dates=tmpout["dates"],
            noise_filter=False,
            seed=12345
        )
        assert len(errs) == 0
        assert len(results) == 2
        assert [r["file_id"] for r in results] == tmpout["file_ids"]
        df = pd.read_parquet(outpath)
        assert len(df) == 20000
        assert len(df["date"].unique()) == 2
        assert len(df.head(10000)["date"].unique()) == 1
        assert df.head(10000)["date"].unique()[0].isoformat() == "2014-07-04T00:00:02+00:00"
        assert df.tail(10000)["date"].unique()[0].isoformat() == "2014-07-04T00:03:02+00:00"

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

        df = pd.read_parquet(outpath)
        assert (
            len(df[(df["D1"] == 0) & (df["D2"] == 0) & (df["fsc_small"] == 0)]) > 0
        )
        gb = df.groupby("file_id")
        assert gb.ngroups == 2
        assert list(gb.groups.keys()) == tmpout["file_ids"]
        assert [len(g) for g in gb.groups.values()] == [20000, 20000]
