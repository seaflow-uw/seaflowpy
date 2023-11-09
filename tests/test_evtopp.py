from builtins import str
from builtins import object
import gzip
import io
import os
import shutil
import sqlite3
import numpy as np
import numpy.testing as npt
import pandas as pd
import pandas.testing as pdt
import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name

@pytest.fixture()
def evt_df():
    return sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00")["df"]


@pytest.fixture()
def params():
    # Params created with popcycle function
    # create.filter.params(740, 33759, 19543, 19440)
    # taking the 50.0 quantile values.
    return pd.DataFrame.from_dict({
        "width": [2500, 2500, 2500],
        "notch_small_D1": [0.614, 0.656, 0.698],
        "notch_small_D2": [0.651, 0.683, 0.714],
        "notch_large_D1": [1.183, 1.635, 2.087],
        "notch_large_D2": [1.208, 1.632, 2.056],
        "offset_small_D1": [1418, 0, -1418],
        "offset_small_D2": [1080, 0, -1047],
        "offset_large_D1": [-17791, -33050, -48309],
        "offset_large_D2": [-17724, -32038, -46352],
        "quantile": [2.5, 50.0, 97.5]
    })


@pytest.fixture()
def tmpout(tmpdir):
    """Setup to test complete filter workflow"""
    # Copy db with filtering params
    db_one = str(tmpdir.join("testcruise_one.db"))
    shutil.copyfile("tests/testcruise_paramsonly_one_param.db", db_one)
    os.chmod(db_one, 0o664)  # make the db writeable

    db_plan = str(tmpdir.join("testcruise_plan.db"))
    shutil.copyfile("tests/testcruise_paramsonly_plan.db", db_plan)
    os.chmod(db_plan, 0o664)  # make the db writeable

    evt_path = "tests/testcruise_evt/2014_185/2014-07-04T00-00-02+00-00"
    return {
        "db_one": db_one,
        "db_plan": db_plan,
        "oppdir": tmpdir.join("oppdir"),
        "tmpdir": tmpdir,
        "evt_df": sfp.fileio.read_evt(str(evt_path))["df"],
        "evt_path": evt_path,
        "file_dates": pd.read_parquet("tests/file_dates.parquet")
    }


class TestOpenV1:
    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v1"
        assert list(data["df"]) == sfp.particleops.COLUMNS
        assert (data["df"].dtypes == np.float32).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_metadata(self, benchmark):
        data = benchmark(sfp.fileio.read_evt_metadata, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00")
        assert data["rowcnt"] == 500000
        assert data["version"] == "v1"
        assert data["colcnt"] == len(sfp.particleops.COLUMNS)

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_uint16(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00", dtype=np.uint16)
        assert len(data["df"].index) == 500000
        assert data["version"] == "v1"
        assert list(data["df"]) == sfp.particleops.COLUMNS
        assert (data["df"].dtypes == np.uint16).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_float64(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00", dtype=np.float64)
        assert len(data["df"].index) == 500000
        assert data["version"] == "v1"
        assert list(data["df"]) == sfp.particleops.COLUMNS
        assert (data["df"].dtypes == np.float64).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_gz(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00.gz")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v1"
        assert list(data["df"]) == sfp.particleops.COLUMNS
    
    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_zst(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt/2021_014/2021-01-14T00-21-03+00-00.zst")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v1"
        assert list(data["df"]) == sfp.particleops.COLUMNS

    def test_read_evt_truncated_gz(self, tmpout):
        truncpath = tmpout["tmpdir"] / "2014-07-04T00-03-02+00-00.gz"
        with open("tests/testcruise_evt/2014_185/2014-07-04T00-03-02+00-00.gz", "rb") as infh:
            with open(truncpath, "wb") as outfh:
                outfh.write(infh.read(400000))
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt(truncpath)

    def test_read_evt_empty(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_evt_zero_header(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_evt_short_header(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-12-02+00-00")

    def test_read_evt_more_data_than_header_count(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-21-02+00-00")

    def test_read_evt_less_data_than_header_count(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt/2014_185/2014-07-04T00-27-02+00-00")


class TestOpenV2:
    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_v2(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v2"
        assert list(data["df"]) == sfp.particleops.COLUMNS2
        assert (data["df"].dtypes == np.float32).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_metadata_v2(self, benchmark):
        data = benchmark(sfp.fileio.read_evt_metadata, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00")
        assert data["rowcnt"] == 500000
        assert data["version"] == "v2"
        assert data["colcnt"] == len(sfp.particleops.COLUMNS2)

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_v2_uint16(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00", dtype=np.uint16)
        assert len(data["df"].index) == 500000
        assert data["version"] == "v2"
        assert list(data["df"]) == sfp.particleops.COLUMNS2
        assert (data["df"].dtypes == np.uint16).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_v2_float64(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00", dtype=np.float64)
        assert len(data["df"].index) == 500000
        assert data["version"] == "v2"
        assert list(data["df"]) == sfp.particleops.COLUMNS2
        assert (data["df"].dtypes == np.float64).all()

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_gz_v2(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00.gz")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v2"
        assert list(data["df"]) == sfp.particleops.COLUMNS2

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_valid_zst_v2(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_v2/2021_014/2021-01-14T00-21-03+00-00.zst")
        assert len(data["df"].index) == 500000
        assert data["version"] == "v2"
        assert list(data["df"]) == sfp.particleops.COLUMNS2

    def test_read_evt_truncated_gz_v2(self, tmpout):
        truncpath = tmpout["tmpdir"] / "2014-07-04T00-03-02+00-00.gz"
        with open("tests/testcruise_evt_v2/2014_185/2014-07-04T00-03-02+00-00.gz", "rb") as infh:
            with open(truncpath, "wb") as outfh:
                outfh.write(infh.read(100000))
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt(truncpath)

    def test_read_evt_empty_v2(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt_v2/2014_185/2014-07-04T00-06-02+00-00")

    def test_read_evt_zero_header_v2(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt_v2/2014_185/2014-07-04T00-09-02+00-00")

    def test_read_evt_short_header_v2(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt_v2/2014_185/2014-07-04T00-12-02+00-00")

    def test_read_evt_more_data_than_header_count_v2(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt_v2/2014_185/2014-07-04T00-21-02+00-00")

    def test_read_evt_less_data_than_header_count_v2(self):
        with pytest.raises(sfp.errors.FileError):
            _data = sfp.fileio.read_evt("tests/testcruise_evt_v2/2014_185/2014-07-04T00-27-02+00-00")

class TestOpenParquetEVT:
    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_parquet(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_parquet/2021_014/2021-01-14T00-21-03+00-00.parquet")
        assert len(data["df"].index) == 500000
        assert data["version"] == "parquet"
        assert (data["df"].dtypes == np.float32).all()
        assert list(data["df"]) == sfp.particleops.REDUCED_COLUMNS

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_metadata_parquet(self, benchmark):
        data = benchmark(sfp.fileio.read_evt_metadata, "tests/test_evt_read_benchmark/evt_parquet/2021_014/2021-01-14T00-21-03+00-00.parquet")
        assert data["rowcnt"] == 500000
        assert data["version"] == "parquet"
        assert data["colcnt"] == len(sfp.particleops.REDUCED_COLUMNS)

    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_parquet_uint16(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_parquet/2021_014/2021-01-14T00-21-03+00-00.parquet", dtype=np.uint16)
        assert len(data["df"].index) == 500000
        assert data["version"] == "parquet"
        assert (data["df"].dtypes == np.uint16).all()
        assert list(data["df"]) == sfp.particleops.REDUCED_COLUMNS
    
    @pytest.mark.benchmark(group="evt-read")
    def test_read_evt_parquet_float64(self, benchmark):
        data = benchmark(sfp.fileio.read_evt, "tests/test_evt_read_benchmark/evt_parquet/2021_014/2021-01-14T00-21-03+00-00.parquet", dtype=np.float64)
        assert len(data["df"].index) == 500000
        assert data["version"] == "parquet"
        assert (data["df"].dtypes == np.float64).all()
        assert list(data["df"]) == sfp.particleops.REDUCED_COLUMNS

class TestFilter:
    def test_mark_focused_no_params(self, evt_df):
        with pytest.raises(ValueError):
            _df = sfp.particleops.mark_focused(evt_df, None)

    def test_mark_focused_empty_params(self, evt_df):
        with pytest.raises(ValueError):
            _df = sfp.particleops.mark_focused(evt_df, pd.DataFrame.from_dict({}))

    @pytest.mark.benchmark(group="evt-filter")
    def test_mark_focused_with_set_params(self, evt_df, params, benchmark):
        orig_df = evt_df.copy()  # make a copy before any possible modifications

        # Should not modify original
        new_evt_df = benchmark(sfp.particleops.mark_focused, evt_df, params)
        assert not (new_evt_df is evt_df)  # returned a new dataframe
        assert orig_df.equals(evt_df)  # original dataframe is the unmodified
        assert len(new_evt_df.index) == 40000
        assert len(new_evt_df[new_evt_df["q2.5"]].index) == 423
        assert len(new_evt_df[new_evt_df["q50"]].index) == 107
        assert len(new_evt_df[new_evt_df["q97.5"]].index) == 85
        assert len(sfp.particleops.select_focused(new_evt_df).index) == 426

    def test_mark_focused_with_set_params_inplace(self, evt_df, params, benchmark):
        orig_df = evt_df.copy()  # make a copy before any possible modifications

        # Should modify original
        new_evt_df = benchmark(sfp.particleops.mark_focused, evt_df, params, inplace=True)
        assert new_evt_df is evt_df  # returned the same dataframe
        assert not orig_df.equals(evt_df)  # original dataframe is the modified
        assert len(new_evt_df.index) == 40000
        assert len(new_evt_df[new_evt_df["q2.5"]].index) == 423
        assert len(new_evt_df[new_evt_df["q50"]].index) == 107
        assert len(new_evt_df[new_evt_df["q97.5"]].index) == 85
        assert len(sfp.particleops.select_focused(new_evt_df).index) == 426

    def test_noise_filter(self, evt_df):
        """Events with zeroes in all of D1, D2, and fsc_small are noise"""
        # There are events which could be considered noise (no signal in any of
        # D1, D2, or fsc_small
        assert np.any((evt_df["D1"] == 0) & (evt_df["D2"] == 0) & (evt_df["fsc_small"] == 0)) == True

        noise = sfp.particleops.mark_noise(evt_df)
        assert noise.sum() == 72
        signal_df = evt_df[~noise]

        # Correct event count
        assert len(signal_df.index) == 39928

        # No events are all zeroes D1, D2, and fsc_small
        assert np.any((signal_df["D1"] == 0) & (signal_df["D2"] == 0) & (signal_df["fsc_small"] == 0)) == False

    def test_saturation_filter(self, evt_df):
        """Events with max D1 or max D2"""
        sat = sfp.particleops.mark_saturated(evt_df)
        assert sat.sum() == 211

    def test_saturation_filter_custom_columns(self, evt_df):
        """Events with max D2"""
        sat = sfp.particleops.mark_saturated(evt_df, cols=["D2"])
        assert sat.sum() == 210

    def test_all_quantiles(self):
        df = pd.DataFrame(
            {
                "x": [True, True, True],
                "q2.5": [True, False, True],
                "q50": [True, True, True],
                "q97.5": [False, True, False]
            }
        )
        assert sfp.particleops.all_quantiles(df) == True

        df = pd.DataFrame(
            {
                "x": [True, True, True],
                "q2.5": [True, False, True],
                "q50": [True, True, True],
                "q97.5": [False, False, False]
            }
        )
        assert sfp.particleops.all_quantiles(df) == False


class TestTransform:
    def test_linearize_four_values(self):
        input_df = pd.DataFrame({
            "fsc_small": [32768, 65536],
            "D1": [0, 1]
        })
        output_df = pd.DataFrame({
            "fsc_small": [56.234132519, 3162.2776601684],
            "D1": [1, 1.0001229789]
        })
        npt.assert_array_almost_equal(
            sfp.particleops.linearize_particles(input_df, columns=["fsc_small", "D1"]),
            output_df
        )

    def test_linearize_copy(self, evt_df):
        orig_df = evt_df.copy()
        npt.assert_array_equal(orig_df, evt_df)
        t_df = sfp.particleops.linearize_particles(evt_df, columns=["fsc_small", "D1"])
        assert t_df is not evt_df
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_df)

    def test_log_four_values(self):
        input_df = pd.DataFrame({
            "fsc_small": [56.234132519, 3162.2776601684],
            "D1": [1, 1.0001229789]
        })
        output_df = pd.DataFrame({
            "fsc_small": [32768, 65536],
            "D1": [0, 1]
        })
        npt.assert_array_almost_equal(
            sfp.particleops.log_particles(input_df, columns=["fsc_small", "D1"]),
            output_df
        )

    def test_log_copy(self, evt_df):
        evt_df = sfp.particleops.linearize_particles(evt_df, columns=["fsc_small", "D1"])
        orig_df = evt_df.copy()
        npt.assert_array_equal(orig_df, evt_df)
        t_df = sfp.particleops.log_particles(evt_df, columns=["fsc_small", "D1"])
        assert t_df is not evt_df
        with pytest.raises(AssertionError):
            npt.assert_array_equal(orig_df, t_df)


class TestOutput:
    def test_sqlite3_opp_counts_and_params(self, tmpout, params):
        sf_file = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        df = tmpout["evt_df"]
        df = sfp.particleops.mark_focused(df, params, inplace=True)

        raw_count = len(df.index)
        signal_count = len(df[df["noise"] == False].index)

        vals = sfp.db.prep_opp(sf_file.file_id, df, raw_count, signal_count, "UUID")
        sfp.db.save_opp_to_db(vals, tmpout["db_one"])
        con = sqlite3.connect(tmpout["db_one"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        try:
            opp_evt_ratio = len(df[df["q50"]].index) / len(df[df["noise"] == False].index)
        except ZeroDivisionError:
            opp_evt_ratio = 0.0

        assert sf_file.file_id == sqlitedf["file"][1]
        assert sqlitedf["filter_id"][1] == "UUID"
        assert sqlitedf["quantile"][1] == 50
        npt.assert_array_equal(
            [107, 39928, 40000, opp_evt_ratio],
            sqlitedf[["opp_count", "evt_count", "all_count", "opp_evt_ratio"]].values[1]
        )


    def test_sqlite3_opp_counts_and_params_empty(self, tmpout, params):
        sf_file = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        df = sfp.particleops.empty_df()
        df = sfp.particleops.mark_focused(df, params, inplace=True)

        raw_count = len(df.index)
        signal_count = len(df[df["noise"] == False].index)

        vals = sfp.db.prep_opp(sf_file.file_id, df, raw_count, signal_count, "UUID")
        sfp.db.save_opp_to_db(vals, tmpout["db_one"])
        con = sqlite3.connect(tmpout["db_one"])
        sqlitedf = pd.read_sql_query("SELECT * FROM opp", con)

        assert sf_file.file_id == sqlitedf["file"][1]
        assert sqlitedf["filter_id"][1] == "UUID"
        assert sqlitedf["quantile"][1] == 50
        npt.assert_array_equal(
            [0, 0, 0, 0.0],
            sqlitedf[["opp_count", "evt_count", "all_count", "opp_evt_ratio"]].values[1]
        )

    def test_binary_evt_output(self, tmpout):
        sfile = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        evtdir = tmpout["tmpdir"] / "evtdir"
        evt_df = sfp.fileio.read_evt(tmpout["evt_path"])["df"]

        # Output to binary uncompressed file
        sfp.fileio.write_evt_labview(evt_df, sfile.file_id, evtdir, gz=False)
        # Make sure EVT binary file written can be read back as EVT and
        # DataFrame is the the same
        out_evt_path = evtdir / sfile.file_id
        reread_evt_df = sfp.fileio.read_evt(out_evt_path)["df"]
        npt.assert_array_equal(evt_df, reread_evt_df)
        # Check that output evt binary file matches input file
        input_evt = io.open(tmpout["evt_path"], "rb").read()
        new_evt = io.open(out_evt_path, "rb").read()
        assert input_evt == new_evt

    def test_binary_evt_output_gz(self, tmpout):
        sfile = sfp.seaflowfile.SeaFlowFile(tmpout["evt_path"])
        evtdir = tmpout["tmpdir"] / "evtdir"
        evt_df = sfp.fileio.read_evt(tmpout["evt_path"])["df"]

        # Output to gzipped binary file
        sfp.fileio.write_evt_labview(evt_df, sfile.file_id, evtdir)
        # Make sure EVT binary file written can be read back as EVT and
        # DataFrame is the the same
        out_evt_path = evtdir / (sfile.file_id + ".gz")
        reread_evt_df = sfp.fileio.read_evt(out_evt_path)["df"]
        npt.assert_array_equal(evt_df, reread_evt_df)
        # Check that output evt binary file matches input file
        input_evt = io.open(tmpout["evt_path"], "rb").read()
        new_evt = gzip.open(out_evt_path).read()
        assert input_evt == new_evt


class TestMultiFileFilter(object):
    @pytest.mark.parametrize("jobs", [1, 2])
    def test_multi_file_filter_local(self, tmpout, jobs):
        """Test multi-file filtering and ensure output can be read back OK"""
        # python setup.py test doesn't play nice with pytest and
        # multiprocessing, so we use one core here
        sfp.filterevt.filter_evt_files(
            tmpout["file_dates"],
            dbpath=tmpout["db_one"],
            opp_dir=tmpout["oppdir"],
            worker_count=jobs
        )

        opp_dfs = [
            pd.read_parquet(tmpout["oppdir"] / "2014-07-04T00-00-00+00-00.1H.opp.parquet"),
            pd.read_parquet(tmpout["oppdir"] / "2014-07-04T01-00-00+00-00.1H.opp.parquet")
        ]
        expected_opp_dfs = [
            pd.read_parquet("tests/testcruise_opp_one_param/2014-07-04T00-00-00+00-00.1H.opp.parquet"),
            pd.read_parquet("tests/testcruise_opp_one_param/2014-07-04T01-00-00+00-00.1H.opp.parquet")
        ]
        pdt.assert_frame_equal(opp_dfs[0], expected_opp_dfs[0], check_exact=False)
        pdt.assert_frame_equal(opp_dfs[1], expected_opp_dfs[1], check_exact=False)

        # Check numbers stored in opp table are correct
        opp_table = sfp.db.get_opp_table(tmpout["db_one"])
        expected_opp_table = sfp.db.get_opp_table("tests/testcruise_full_one_param.db")

        pdt.assert_frame_equal(opp_table, expected_opp_table, check_exact=False)

        # Check that outlier table has entry for every file
        outlier_table = sfp.db.get_outlier_table(tmpout["db_one"])
        expected_outlier_table = sfp.db.get_outlier_table("tests/testcruise_full_one_param.db")
        pdt.assert_frame_equal(outlier_table, expected_outlier_table)

    @pytest.mark.parametrize("jobs", [1, 2])
    def test_multi_file_filter_local_v2(self, tmpout, jobs):
        """Test multi-file filtering on v2 data and ensure output can be read back OK"""
        file_dates = tmpout["file_dates"].copy()
        file_dates["path"] = file_dates["path_v2"]
        sfp.filterevt.filter_evt_files(
            file_dates,
            dbpath=tmpout["db_plan"],
            opp_dir=str(tmpout["oppdir"]),
            worker_count=jobs
        )

        opp_dfs = [
            pd.read_parquet(tmpout["oppdir"] / "2014-07-04T00-00-00+00-00.1H.opp.parquet"),
            pd.read_parquet(tmpout["oppdir"] / "2014-07-04T01-00-00+00-00.1H.opp.parquet")
        ]
        expected_opp_dfs = [
            pd.read_parquet("tests/testcruise_opp_plan/2014-07-04T00-00-00+00-00.1H.opp.parquet"),
            pd.read_parquet("tests/testcruise_opp_plan/2014-07-04T01-00-00+00-00.1H.opp.parquet")
        ]
        pdt.assert_frame_equal(opp_dfs[0], expected_opp_dfs[0], check_exact=False)
        pdt.assert_frame_equal(opp_dfs[1], expected_opp_dfs[1], check_exact=False)

        # Check numbers stored in opp table are correct
        opp_table = sfp.db.get_opp_table(tmpout["db_plan"])
        expected_opp_table = sfp.db.get_opp_table("tests/testcruise_full_plan.db")

        pdt.assert_frame_equal(opp_table, expected_opp_table, check_exact=False)

        # Check that outlier table has entry for every file
        outlier_table = sfp.db.get_outlier_table(tmpout["db_plan"])
        expected_outlier_table = sfp.db.get_outlier_table("tests/testcruise_full_plan.db")
        pdt.assert_frame_equal(outlier_table, expected_outlier_table)

    @pytest.mark.parametrize("jobs", [1, 2])
    def test_multi_file_filter_local_v2_with_per_file_limit(self, tmpout, jobs):
        """Test multi-file filtering on v2 data with per-file event max and ensure output can be read back OK"""
        file_dates = tmpout["file_dates"].copy()
        file_dates["path"] = file_dates["path_v2"]
        sfp.filterevt.filter_evt_files(
            file_dates,
            dbpath=tmpout["db_plan"],
            opp_dir=str(tmpout["oppdir"]),
            worker_count=jobs,
            max_particles_per_file=1
        )

        assert not (tmpout["oppdir"] / "2014-07-04T00-00-00+00-00.1H.opp.parquet").exists()
        assert not (tmpout["oppdir"] / "2014-07-04T01-00-00+00-00.1H.opp.parquet").exists()

        # Check data stored in opp table are correct
        opp_table = sfp.db.get_opp_table(tmpout["db_plan"])
        assert len(opp_table) == 24
        assert opp_table["all_count"].sum() == 600000
        assert opp_table["evt_count"].sum() == 0
        assert opp_table["opp_count"].sum() == 0

        # Check that outlier table has entry for every file
        outlier_table = sfp.db.get_outlier_table(tmpout["db_plan"])
        expected_outlier_table = sfp.db.get_outlier_table("tests/testcruise_full_plan.db")
        pdt.assert_frame_equal(outlier_table, expected_outlier_table)
