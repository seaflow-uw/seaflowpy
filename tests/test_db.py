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
    filter_tsv = "tests/testcruise.filter_params.filter.tsv"
    filter_plan_tsv = "tests/testcruise.filter_params.filter_plan.tsv"
    filter2_tsv = "tests/testcruise.filter_params.filter2.tsv"
    filter_plan2_tsv = "tests/testcruise.filter_params.filter_plan2.tsv"
    types = defaultdict(
        lambda: "float64[pyarrow]",
        cruise=pd.ArrowDtype(pa.string()),
        instrument=pd.ArrowDtype(pa.string()),
        id=pd.ArrowDtype(pa.string()),
        date=pd.ArrowDtype(pa.string())
    )

    # Import one set of params
    expect_filter = pd.read_csv(filter_tsv, sep="\t", dtype=types, dtype_backend="pyarrow")
    expect_filter = expect_filter.drop(columns=["instrument", "cruise"])
    expect_filter_plan = pd.read_csv(filter_plan_tsv, sep="\t", dtype=pd.ArrowDtype(pa.string()), dtype_backend="pyarrow")
    sfp.db.import_filter_params(filter_tsv, filter_plan_tsv, testdb)
    got_filter = sfp.db.get_filter_table(testdb)
    got_filter_plan = sfp.db.get_filter_plan_table(testdb)
    pdt.assert_frame_equal(got_filter, expect_filter)
    pdt.assert_frame_equal(got_filter_plan, expect_filter_plan)

    # Import another set, which should replace the first completely
    expect_filter2 = pd.read_csv(filter2_tsv, sep="\t", dtype=types, dtype_backend="pyarrow")
    expect_filter2 = expect_filter2.drop(columns=["instrument", "cruise"])
    expect_filter_plan2 = pd.read_csv(filter_plan2_tsv, sep="\t", dtype=pd.ArrowDtype(pa.string()), dtype_backend="pyarrow")
    sfp.db.import_filter_params(filter2_tsv, filter_plan2_tsv, testdb)
    got_filter2 = sfp.db.get_filter_table(testdb)
    got_filter_plan2 = sfp.db.get_filter_plan_table(testdb)
    pdt.assert_frame_equal(got_filter2, expect_filter2)
    pdt.assert_frame_equal(got_filter_plan2, expect_filter_plan2)


def test_import_gating_params(test_data):
    # Disabling dtype checks for this test. Between sqlite3 not enforcing types,
    # and Python, R, and to some extent Pandas/SQLAlchemy loosely using types,
    # it's tricky to coerce dataframes into and out of the database to the exact
    # same dtypes. Match only on values instead.
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_gating_params(
        "tests/testcruise.gating_params.gating.tsv",
        "tests/testcruise.gating_params.poly.tsv",
        "tests/testcruise.gating_params.gating_plan.tsv",
        testdb
    )
    got_gating_df = sfp.db.read_table("gating", testdb)
    expect_gating_df = pd.read_csv("tests/testcruise.gating_params.gating.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_gating_df, expect_gating_df, check_dtype=False)

    got_poly_df = sfp.db.read_table("poly", testdb)
    expect_poly_df = pd.read_csv("tests/testcruise.gating_params.poly.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_poly_df, expect_poly_df, check_dtype=False)

    got_gating_plan_df = sfp.db.read_table("gating_plan", testdb)
    expect_gating_plan_df = pd.read_csv("tests/testcruise.gating_params.gating_plan.tsv", sep="\t", dtype_backend="pyarrow")
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


def test_export_filter_params(test_data):
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_filter_params(
        "tests/testcruise.filter_params.filter.tsv",
        "tests/testcruise.filter_params.filter_plan.tsv",
        testdb
    )
    outprefix_path = test_data["tmpdir"] / "outprefix"
    sfp.db.export_filter_params(testdb, outprefix_path)
    
    got_filter_df = pd.read_csv(f"{outprefix_path}.filter.tsv", sep="\t", dtype_backend="pyarrow")
    expect_filter_df = pd.read_csv("tests/testcruise.filter_params.filter.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_filter_df, expect_filter_df, check_dtype=False)

    got_filter_plan_df = pd.read_csv(f"{outprefix_path}.filter_plan.tsv", sep="\t", dtype_backend="pyarrow")
    expect_filter_plan_df = pd.read_csv("tests/testcruise.filter_params.filter_plan.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_filter_plan_df, expect_filter_plan_df, check_dtype=False)

    # Try again after removing filter_plan table, should create one from first
    # sfl date
    outprefix_path2 = test_data["tmpdir"] / "outprefix2"
    engine = sqlalchemy.create_engine(f"sqlite:///{testdb}")
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sqlalchemy.text("delete from filter_plan"))
            conn.execute(sqlalchemy.text("delete from filter where id = 'a78bfaf2-0f84-4da9-bd19-e518e4e4529b'"))
    engine.dispose()
    df_tmp = pd.read_sql_table("filter_plan", f"sqlite:///{testdb}")
    assert len(df_tmp) == 0
    df_tmp = pd.read_sql_table("filter", f"sqlite:///{testdb}")
    assert len(df_tmp["id"].unique()) == 1
    sfp.db.export_filter_params(testdb, outprefix_path2)
    got_filter_df2 = pd.read_csv(f"{outprefix_path2}.filter.tsv", sep="\t", dtype_backend="pyarrow")
    expect_filter_df2 = pd.read_csv("tests/testcruise.filter_params.filter.tsv", sep="\t", dtype_backend="pyarrow")
    expect_filter_df2 = expect_filter_df2[expect_filter_df2["id"] != "a78bfaf2-0f84-4da9-bd19-e518e4e4529b"]
    pdt.assert_frame_equal(got_filter_df2, expect_filter_df2, check_dtype=False)

    got_filter_plan_df2 = pd.read_csv(f"{outprefix_path2}.filter_plan.tsv", sep="\t", dtype_backend="pyarrow")
    expect_filter_plan_df2 = sfp.db.create_filter_plan(testdb)
    pdt.assert_frame_equal(got_filter_plan_df2, expect_filter_plan_df2, check_dtype=False)


def test_export_gating_params(test_data):
    testdb = test_data["db_sfl_meta"]
    sfp.db.import_gating_params(
        "tests/testcruise.gating_params.gating.tsv",
        "tests/testcruise.gating_params.poly.tsv",
        "tests/testcruise.gating_params.gating_plan.tsv",
        testdb
    )
    outprefix_path = test_data["tmpdir"] / "outprefix"
    sfp.db.export_gating_params(testdb, outprefix_path)
    
    got_gating_df = pd.read_csv(f"{outprefix_path}.gating.tsv", sep="\t", dtype_backend="pyarrow")
    expect_gating_df = pd.read_csv("tests/testcruise.gating_params.gating.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_gating_df, expect_gating_df, check_dtype=False)

    got_poly_df = pd.read_csv(f"{outprefix_path}.poly.tsv", sep="\t", dtype_backend="pyarrow")
    expect_poly_df = pd.read_csv("tests/testcruise.gating_params.poly.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_poly_df, expect_poly_df, check_dtype=False)

    got_gating_plan_df = pd.read_csv(f"{outprefix_path}.gating_plan.tsv", sep="\t", dtype_backend="pyarrow")
    expect_gating_plan_df = pd.read_csv("tests/testcruise.gating_params.gating_plan.tsv", sep="\t", dtype_backend="pyarrow")
    pdt.assert_frame_equal(got_gating_plan_df, expect_gating_plan_df, check_dtype=False)

    # Try again after removing gating_plan table, should produce no output
    # and throw since no vct data to fall back on
    outprefix_path2 = test_data["tmpdir"] / "outprefix2"
    engine = sqlalchemy.create_engine(f"sqlite:///{testdb}")
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sqlalchemy.text("drop table gating_plan"))
    engine.dispose()
    with pytest.raises(ValueError):
        _ = pd.read_sql_table("gating_plan", f"sqlite:///{testdb}")
    with pytest.raises(sfp.errors.SeaFlowpyError):
        sfp.db.export_gating_params(testdb, outprefix_path2)
    assert not Path(f"{outprefix_path2}.gating.tsv").exists()
    assert not Path(f"{outprefix_path2}.poly.tsv").exists()
    assert not Path(f"{outprefix_path2}.gating_plan.tsv").exists()


def test_export_outlier(test_data):
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
    df = pd.read_csv(outfile2, sep="\t", dtype_backend="pyarrow")
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
