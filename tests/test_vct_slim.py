import pandas as pd
import pytest
import seaflowpy as sfp

# pylint: disable=redefined-outer-name


@pytest.fixture()
def vct_df():
    return pd.read_parquet("tests/testcruise_vct_slim/2021-05-15T21-00-00+00-00.1H.vct.parquet")


def test_quantile_only(vct_df):
    vct_mod = sfp.vct.curate(vct_df, quantile="50")
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe"]
    assert len(vct_mod) == vct_df["q50"].sum()
    assert (vct_mod["pop"].to_numpy() == vct_df.loc[vct_df["q50"], "pop_q50"].to_numpy()).all()


def test_refracs(vct_df):
    refracs = pd.DataFrame({
        "cruise": ["HOT330"],
        "prochloro": ["lwr"],
        "synecho": ["lwr"],
        "beads": ["mid"],
        "unknown": ["mid"],
        "croco": ["upr"],  # not in file
        "picoeuk": ["upr"]
    })
    vct_mod = sfp.vct.curate(vct_df, quantile="50", refracs=refracs)
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe", "diam", "Qc"]
    assert len(vct_mod) == vct_df["q50"].sum()
    for pop in refracs.columns[1:]:
        idx = vct_mod["pop"] == pop
        if idx.any():
            expected_diam = vct_df.loc[vct_df["q50"] & (vct_df["pop_q50"] == pop), f"diam_{refracs[pop].values[0]}_q50"]
            expected_Qc = vct_df.loc[vct_df["q50"] & (vct_df["pop_q50"] == pop), f"Qc_{refracs[pop].values[0]}_q50"]
            assert (vct_mod.loc[idx, "diam"].to_numpy() == expected_diam.to_numpy()).all()
            assert (vct_mod.loc[idx, "Qc"].to_numpy() == expected_Qc.to_numpy()).all()


def test_ignore_dates(vct_df):
    ignore_dates = vct_df["date"].unique()[:2]
    vct_mod = sfp.vct.curate(vct_df, quantile="50", ignore_dates=ignore_dates)
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe"]
    assert len(vct_mod) == vct_df["q50"].sum() - vct_df.loc[vct_df["date"].isin(ignore_dates), "q50"].sum()
    assert not vct_mod["date"].isin(ignore_dates).any()


def test_single_refrac(vct_df):
    vct_mod = sfp.vct.curate(vct_df, quantile="50", refracs="mid")
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe", "diam", "Qc"]
    assert len(vct_mod) == vct_df["q50"].sum()
    expected_diam = vct_df.loc[vct_df["q50"], "diam_mid_q50"]
    expected_Qc = vct_df.loc[vct_df["q50"], "Qc_mid_q50"]
    assert (vct_mod["diam"].to_numpy() == expected_diam.to_numpy()).all()
    assert (vct_mod["Qc"].to_numpy() == expected_Qc.to_numpy()).all()


def test_vct_from_file_path():
    """Test reading vct from file path (str)"""
    vct_mod = sfp.vct.curate(
        "tests/testcruise_vct_slim/2021-05-15T21-00-00+00-00.1H.vct.parquet",
        quantile="50"
    )
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe"]
    assert len(vct_mod) > 0


def test_vct_from_pathlib_path():
    """Test reading vct from pathlib.Path"""
    from pathlib import Path
    vct_mod = sfp.vct.curate(
        Path("tests/testcruise_vct_slim/2021-05-15T21-00-00+00-00.1H.vct.parquet"),
        quantile="50"
    )
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe"]
    assert len(vct_mod) > 0


def test_ignore_dates_as_series(vct_df):
    """Test ignore_dates with pd.Series"""
    ignore_dates = pd.Series(vct_df["date"].unique()[:2])
    vct_mod = sfp.vct.curate(vct_df, quantile="50", ignore_dates=ignore_dates)
    assert not vct_mod["date"].isin(ignore_dates).any()


def test_no_mutation_of_original_dataframe(vct_df):
    """Verify original DataFrame is not modified"""
    vct_original = vct_df.copy()
    sfp.vct.curate(vct_df, quantile="50", refracs="mid")
    pd.testing.assert_frame_equal(vct_df, vct_original)


def test_population_not_in_refracs(vct_df):
    """Test when vct has population not in refracs - should have NaN"""
    refracs = pd.DataFrame({
        "cruise": ["HOT330"],
        "prochloro": ["lwr"],
        # Missing other populations
    })
    with pytest.raises(ValueError, match="Some populations in VCT data were not found in refracs DataFrame."):
        _ = sfp.vct.curate(vct_df, quantile="50", refracs=refracs)


def test_all_rows_filtered_by_ignore_dates(vct_df):
    """Test when all dates are ignored - should return empty DataFrame"""
    ignore_dates = vct_df["date"].unique()
    vct_mod = sfp.vct.curate(vct_df, quantile="50", ignore_dates=ignore_dates)
    assert len(vct_mod) == 0
    assert vct_mod.columns.tolist() == ["date", "pop", "fsc_small", "chl_small", "pe"]
