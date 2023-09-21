import shutil
import sqlalchemy
from collections import defaultdict
from pathlib import Path
import pandas as pd
import pandas.testing as pdt
import pyarrow as pa
import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name

@pytest.fixture()
def test_data(tmpdir):
    # Copy db with filtering params
    r = {
        "db_sfl_meta": tmpdir / "testcruise.db",
        "db_filter": tmpdir / "testcruise_filterparams.db",
        "db_empty": tmpdir / "testcruise_empty.db",
        "tmpdir": tmpdir
    }
    shutil.copyfile("tests/testcruise_sfl_metadata.db", r["db_sfl_meta"])
    shutil.copyfile("tests/testcruise_paramsonly_one_param.db", r["db_filter"])
    sfp.db.create_db(r["db_empty"])
    return r


def test_import_filter_params(test_data):
    testdb = test_data["db_sfl_meta"]
    csvpath = "tests/filterparams.csv"
    types = defaultdict(
        lambda: "float64[pyarrow]",
        cruise=pd.ArrowDtype(pa.string()),
        instrument=pd.ArrowDtype(pa.string())
    )

    # Import one set of params
    expect = pd.read_csv(csvpath, dtype=types, dtype_backend="pyarrow")
    expect = expect.drop(columns=["instrument", "cruise"])
    expect.columns = [c.replace('.', '_') for c in expect.columns]
    id1 = sfp.db.import_filter_params(csvpath, testdb, plan=False, clear=False)
    got = sfp.db.get_filter_table(testdb)
    got_ = got.drop(columns=["id", "date"])
    pdt.assert_frame_equal(got_, expect)
    assert len(got["id"].unique()) == 1
    assert list(got["id"].unique()) == [id1]

    # Append a second set of params
    expect2 = (
        pd.concat([expect, expect], ignore_index=True)
        .sort_values(["quantile", "beads_fsc_small"])
        .reset_index(drop=True)
    )
    id2 = sfp.db.import_filter_params(csvpath, testdb, plan=False, clear=False)
    got = sfp.db.get_filter_table(testdb)
    got_ = (
        got.drop(columns=["id", "date"])
        .sort_values(["quantile", "beads_fsc_small"])
        .reset_index(drop=True)
    )
    pdt.assert_frame_equal(got_, expect2)
    assert len(got["id"].unique()) == 2
    assert sorted(list(got["id"].unique())) == sorted([id1, id2])

    # Import a third set of params replacing all previous params, and create a
    # filter_plan
    id3 = sfp.db.import_filter_params(csvpath, testdb, plan=True, clear=True)
    got = sfp.db.get_filter_table(testdb)
    got_ = got.drop(columns=["id", "date"])
    pdt.assert_frame_equal(got_, expect)
    assert len(got["id"].unique()) == 1
    assert list(got["id"].unique()) == [id3]

    got = sfp.db.get_filter_plan_table(testdb)
    expect = pd.DataFrame(
        {"start_date": "2014-07-04T00:00:02+00:00", "filter_id": [id3]},
        dtype=pd.ArrowDtype(pa.string())
    )
    print(got.info())
    print(expect.info())
    pdt.assert_frame_equal(got, expect)


def test_import_filter_params_two_sets(test_data):
    testdb = test_data["db_sfl_meta"]
    types = defaultdict(
        lambda: "float64[pyarrow]",
        cruise=pd.ArrowDtype(pa.string()),
        instrument=pd.ArrowDtype(pa.string())
    )
    expect = pd.read_csv("tests/filterparams.csv", dtype=types, dtype_backend="pyarrow")
    expect = expect.drop(columns=["instrument", "cruise"])
    expect.columns = [c.replace('.', '_') for c in expect.columns]
    id_ = sfp.db.import_filter_params("tests/filterparams2.csv", testdb, plan=True, clear=False)
    got = sfp.db.get_filter_table(testdb)
    got_ = got.drop(columns=["id", "date"])
    pdt.assert_frame_equal(got_, expect)
    assert len(got["id"].unique()) == 1
    assert got["id"][0] == id_


def test_import_gating_params(test_data):
    # Disabling dtype checks for this test. Between sqlite3 not enforcing types,
    # and Python, R, and to some extent Pandas/SQLAlchemy loosely using types,
    # it's tricky to coerce dataframes into and out of the database to the exact
    # same dtypes. Match only on values instead.
    csv_kwargs = {"sep": "\t", "dtype_backend": "pyarrow"}
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_gating_params(
        "tests/testcruise.gating_params.gating.tsv",
        "tests/testcruise.gating_params.poly.tsv",
        "tests/testcruise.gating_params.gating_plan.tsv",
        testdb
    )
    got_gating_df = sfp.db.read_table("gating", testdb)
    expect_gating_df = pd.read_csv("tests/testcruise.gating_params.gating.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_df, expect_gating_df, check_dtype=False)

    got_poly_df = sfp.db.read_table("poly", testdb)
    expect_poly_df = pd.read_csv("tests/testcruise.gating_params.poly.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_poly_df, expect_poly_df, check_dtype=False)

    got_gating_plan_df = sfp.db.read_table("gating_plan", testdb)
    expect_gating_plan_df = pd.read_csv("tests/testcruise.gating_params.gating_plan.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_plan_df, expect_gating_plan_df, check_dtype=False)


def test_import_sfl_no_cruise_serial(test_data):
    """No cruise or serial in file name or db"""
    testdb = test_data["db_empty"]
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.import_sfl("tests/testcruise.sfl", testdb)


def test_import_sfl_force(test_data):
    testdb = test_data["db_empty"]
    sfp.db.save_df(
        pd.DataFrame([{"cruise": "testcruise", "inst": "740"}]),
        "metadata",
        testdb
    )

    # Fatal error in SFL, will not import and will get two returned errors
    # (one warning, one error)
    got_errs = sfp.db.import_sfl("tests/testcruise-bad-lat.sfl", testdb)
    got_table = sfp.db.read_table("sfl", testdb)
    assert len(got_errs) == 2
    assert len(got_table) == 0

    # Force it anyway
    got_errs = sfp.db.import_sfl("tests/testcruise-bad-lat.sfl", testdb, force=True)
    got_table = sfp.db.read_table("sfl", testdb)
    assert len(got_errs) == 2
    assert len(got_table) == 9


def test_import_sfl_populated_metadata_table(test_data):
    testdb = test_data["db_empty"]

    # Should work with populated metadata table and no file cruise/serial
    sfp.db.save_df(
        pd.DataFrame([{"cruise": "testcruise", "inst": "740"}]),
        "metadata",
        testdb
    )
    got_errs = sfp.db.import_sfl("tests/testcruise.sfl", testdb)
    got_table = sfp.db.read_table("sfl", testdb)
    assert len(got_errs) == 1
    assert len(got_table) == 9

    # Mismatch between metadata table serial and file serial should throw
    sfp.db.save_df(
        pd.DataFrame([{"cruise": "testcruise", "inst": "999"}]),
        "metadata",
        testdb,
        clear=True
    )
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.import_sfl("tests/testcruise_740.sfl", testdb)

    # Mismatch between metadata table cruise and file serial should throw
    sfp.db.save_df(
        pd.DataFrame([{"cruise": "aaa", "inst": "740"}]),
        "metadata",
        testdb,
        clear=True
    )
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.import_sfl("tests/testcruise_740.sfl", testdb)


def test_import_sfl_serial_cruise_in_file_name(test_data):
    testdb = test_data["db_empty"]

    # Should work with cruise/serial in file name
    _ = sfp.db.import_sfl("tests/testcruise_740.sfl", testdb)
    got = sfp.db.read_table("sfl", testdb)
    assert len(got) == 9

    # metadata table should be populated based on file name
    expected = pd.DataFrame([{"cruise": "testcruise", "inst": "740"}], dtype=pd.ArrowDtype(pa.string()))
    got = sfp.db.read_table("metadata", testdb)
    pdt.assert_frame_equal(got, expected)


def test_create_filter_plan(test_data):
    testdb = test_data["db_filter"]
    
    # Throw error with two sets of filter params
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.create_filter_plan(testdb)
    
    # Throw error with no filter params
    orig_filts = sfp.db.read_table("filter", testdb)
    sfp.db.save_df(orig_filts.head(0), "filter", testdb, clear=True)  # clear filter table
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.create_filter_plan(testdb)
    
    # Throw error with no sfl table
    one_filt = orig_filts[orig_filts["id"] == "2414efe1-a4ff-46da-a393-9180d6eab149"]
    sfp.db.save_df(one_filt, "filter", testdb)
    orig_sfl = sfp.db.read_table("sfl", testdb)
    sfp.db.save_df(orig_sfl.head(0), "sfl", testdb, clear=True)  # clear sfl table
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.create_filter_plan(testdb)

    # Put sfl table back
    sfp.db.save_df(orig_sfl, "sfl", testdb)

    # Throw error when a filter plan already exists
    with pytest.raises(sfp.errors.SeaFlowpyError):
        _ = sfp.db.create_filter_plan(testdb)

    # Clear filter plan, should work now
    sfp.db.save_df(pd.DataFrame(), "filter_plan", testdb, clear=True)
    got = sfp.db.create_filter_plan(testdb)
    expect = pd.DataFrame(
        {"start_date": ["2014-07-04T00:00:02+00:00"], "filter_id": ["2414efe1-a4ff-46da-a393-9180d6eab149"]},
        dtype=pd.ArrowDtype(pa.string())
    )
    pdt.assert_frame_equal(got, expect)


def test_export_gating_params(test_data):
    csv_kwargs = {"sep": "\t", "dtype_backend": "pyarrow"}
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_gating_params(
        "tests/testcruise.gating_params.gating.tsv",
        "tests/testcruise.gating_params.poly.tsv",
        "tests/testcruise.gating_params.gating_plan.tsv",
        testdb
    )
    outprefix_path = test_data["tmpdir"] / "outprefix"
    sfp.db.export_gating_params(testdb, outprefix_path)
    
    got_gating_df = pd.read_csv(f"{outprefix_path}.gating.tsv", **csv_kwargs)
    expect_gating_df = pd.read_csv("tests/testcruise.gating_params.gating.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_df, expect_gating_df, check_dtype=False)

    got_poly_df = pd.read_csv(f"{outprefix_path}.poly.tsv", **csv_kwargs)
    expect_poly_df = pd.read_csv("tests/testcruise.gating_params.poly.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_poly_df, expect_poly_df, check_dtype=False)

    got_gating_plan_df = pd.read_csv(f"{outprefix_path}.gating_plan.tsv", **csv_kwargs)
    expect_gating_plan_df = pd.read_csv("tests/testcruise.gating_params.gating_plan.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_plan_df, expect_gating_plan_df, check_dtype=False)


def test_export_gating_params(test_data):
    csv_kwargs = {"sep": "\t", "dtype_backend": "pyarrow"}
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_gating_params(
        "tests/testcruise.gating_params.gating.tsv",
        "tests/testcruise.gating_params.poly.tsv",
        "tests/testcruise.gating_params.gating_plan.tsv",
        testdb
    )
    outprefix_path = test_data["tmpdir"] / "outprefix"
    sfp.db.export_gating_params(testdb, outprefix_path)
    
    got_gating_df = pd.read_csv(f"{outprefix_path}.gating.tsv", **csv_kwargs)
    expect_gating_df = pd.read_csv("tests/testcruise.gating_params.gating.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_df, expect_gating_df, check_dtype=False)

    got_poly_df = pd.read_csv(f"{outprefix_path}.poly.tsv", **csv_kwargs)
    expect_poly_df = pd.read_csv("tests/testcruise.gating_params.poly.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_poly_df, expect_poly_df, check_dtype=False)

    got_gating_plan_df = pd.read_csv(f"{outprefix_path}.gating_plan.tsv", **csv_kwargs)
    expect_gating_plan_df = pd.read_csv("tests/testcruise.gating_params.gating_plan.tsv", **csv_kwargs)
    pdt.assert_frame_equal(got_gating_plan_df, expect_gating_plan_df, check_dtype=False)

    # Try again after removing gating_plan table, should produce no output
    # and throw since no vct data to fall back on
    outprefix_path2 = test_data["tmpdir"] / "outprefix2"
    engine = sqlalchemy.create_engine(f"sqlite:///{testdb}")
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("drop table gating_plan"))
    engine.dispose()
    with pytest.raises(sfp.errors.SeaFlowpyError):
        sfp.db.export_gating_params(testdb, outprefix_path2)
    assert not Path(f"{outprefix_path2}.gating.tsv").exists()
    assert not Path(f"{outprefix_path2}.poly.tsv").exists()
    assert not Path(f"{outprefix_path2}.gating_plan.tsv").exists()


def test_export_outlier(test_data):
    csv_kwargs = {"sep": "\t", "dtype_backend": "pyarrow"}
    testdb = test_data["db_empty"]
    sfp.db.save_df(
        pd.DataFrame({"file": ["a", "b", "c"], "flag": [0, 0, 0]}),
        "outlier",
        testdb
    )
    outfile = test_data["tmpdir"] / "outlier.tsv"
    sfp.db.export_outlier(testdb, outfile)
    assert not outfile.exists()

    outfile = test_data["tmpdir"] / "outlier.tsv"
    sfp.db.export_outlier(testdb, outfile, populated=False)
    assert outfile.exists()

    outfile2 = test_data["tmpdir"] / "outlier2.tsv"
    sfp.db.save_df(
        pd.DataFrame({"file": ["a", "b", "c"], "flag": [0, 1, 0]}),
        "outlier",
        testdb,
        clear=True
    )
    sfp.db.export_outlier(testdb, outfile2)
    assert outfile2.exists()
    df = pd.read_csv(outfile2, **csv_kwargs)
    assert df["file"].to_list() == ["a", "b", "c"]
    assert df["flag"].to_list() == [0, 1, 0]


def test_import_outlier(test_data):
    testdb = test_data["db_empty"]
    expect = pd.DataFrame({"file": ["a", "b", "c"], "flag": [0, 1, 0]})
    outfile = test_data["tmpdir"] / "outlier.tsv"
    expect.to_csv(outfile, sep="\t", index=False)
    sfp.db.import_outlier(outfile, testdb)
    got = sfp.db.read_table("outlier", testdb)
    pdt.assert_frame_equal(got, expect, check_dtype=False)


def test_save_df_replace(test_data):
    testdb = test_data["db_empty"]
    df1 = pd.DataFrame({
        "file": ["f1", "f1", "f2", "f3"],
        "all_count": [110, 110, 220, 330],
        "opp_count": [10, 5, 20, 30],
        "evt_count": [100, 100, 200, 300],
        "opp_evt_ratio": [0.1, 0.05, 0.1, 0.1],
        "filter_id": ["a", "a", "a", "a"],
        "quantile": [2.5, 50, 2.5, 2.5]
    })
    sfp.db.save_df(df1, "opp", testdb, clear=True, replace_by_file=False)
    df2 = pd.DataFrame({
        "file": ["f1", "f1", "f4", "f5"],
        "all_count": [110, 110, 220, 330],
        "opp_count": [7, 3, 20, 30],
        "evt_count": [100, 100, 200, 300],
        "opp_evt_ratio": [0.1, 0.05, 0.1, 0.1],
        "filter_id": ["b", "b", "a", "a"],
        "quantile": [2.5, 50, 2.5, 2.5]
    })
    sfp.db.save_df(df2, "opp", testdb, clear=False, replace_by_file=True)
    expect = pd.concat(
        [
            df1[df1["file"].isin(["f2", "f3"])],
            df2[df2["file"].isin(["f1", "f4", "f5"])]
        ],
        ignore_index=True
    )
    got = sfp.db.read_table("opp", testdb)
    pdt.assert_frame_equal(got, expect, check_dtype=False)

    df3 = pd.DataFrame({
        "file": ["f1"],
        "all_count": [110],
        "opp_count": [3],
        "evt_count": [100],
        "opp_evt_ratio": [0.03],
        "filter_id": ["c"],
        "quantile": [97.5]
    })
    sfp.db.save_df(df3, "opp", testdb, clear=False, replace_by_file=False)
    expect = pd.concat([expect, df3], ignore_index=True)
    got = sfp.db.read_table("opp", testdb)
    pdt.assert_frame_equal(got, expect, check_dtype=False)


def test_save_df_clear(test_data):
    testdb = test_data["db_empty"]
    df1 = pd.DataFrame({
        "file": ["f1", "f1"],
        "all_count": [110, 110],
        "opp_count": [10, 5],
        "evt_count": [100, 100],
        "opp_evt_ratio": [0.1, 0.05],
        "filter_id": ["a", "a"],
        "quantile": [2.5, 50]
    })
    sfp.db.save_df(df1, "opp", testdb, clear=True, replace_by_file=False)
    df2 = pd.DataFrame({
        "file": ["f4", "f5"],
        "all_count": [220, 330],
        "opp_count": [20, 30],
        "evt_count": [200, 300],
        "opp_evt_ratio": [0.1, 0.1],
        "filter_id": ["a", "a"],
        "quantile": [2.5, 2.5]
    })
    sfp.db.save_df(df2, "opp", testdb, clear=True, replace_by_file=False)
    got = sfp.db.read_table("opp", testdb)
    pdt.assert_frame_equal(got, df2, check_dtype=False)

    df3 = pd.DataFrame({
        "file": ["f6", "f7"],
        "all_count": [440, 550],
        "opp_count": [40, 50],
        "evt_count": [400, 500],
        "opp_evt_ratio": [0.1, 0.1],
        "filter_id": ["a", "a"],
        "quantile": [2.5, 2.5]
    })
    sfp.db.save_df(df3, "opp", testdb, clear=False, replace_by_file=False)
    got = sfp.db.read_table("opp", testdb)
    expect = pd.concat([df2, df3], ignore_index=True)
    pdt.assert_frame_equal(got, expect, check_dtype=False)
