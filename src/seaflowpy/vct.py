from collections.abc import (
    Mapping,
    Sequence
)
import numpy as np
import pandas as pd

def curate(
    vct: pd.DataFrame, refracs: pd.DataFrame, quantile: str,
    ignore_dates: pd.Series | pd.DataFrame | Sequence | Mapping | None=None
):
    """Select VCT data for one quantile, population refractive indices, and remove ignored dates
    
    This function does not modify the original DataFrame.
    """
    pop_col = f"pop_q{quantile}"
    quant_col = f"q{quantile}"
    vct = vct[vct[quant_col]]
    if ignore_dates:
        vct = vct.loc[~vct["date"].isin(ignore_dates)]
    vct = vct.rename(columns={pop_col: "pop"}).copy()
    vct["diam"] = np.NaN
    vct["Qc"] = np.NaN
    for pop in [c for c in refracs.columns if c != "cruise"]:
        idx = vct["pop"] == pop
        if idx.any():
            vct.loc[idx, "diam"] = vct.loc[idx, f"diam_{refracs[pop].values[0]}_q{quantile}"]
            vct.loc[idx, "Qc"] = vct.loc[idx, f"Qc_{refracs[pop].values[0]}_q{quantile}"]
    return vct.reindex(columns=["date", "pop", "fsc_small", "chl_small", "pe", "diam", "Qc"])


# def grid_vct(vct: pd.DataFrame, grid_bins: pd.DataFrame) -> pd.DataFrame:
#     """Return gridded VCT data"""
#     vct = vct.copy()
#     for dim in [c for c in vct.columns if c in grid_bins.columns]:
#         vct[f"{dim}_coord"] = pd.cut(vct[dim], grid_bins[dim], labels=False, right=False).astype(np.int32) + 1
#     group_cols = ["date", "pop"] + [f"{c}_coord" for c in grid_bins.columns if c != "cruise"]
#     gb = vct.groupby(group_cols, group_keys=False, observed=True)
#     counts = gb.size().to_frame(name="n")
#     counts["Qc_sum"] = gb.agg({"Qc": "sum"})["Qc"]
#     counts = counts.reset_index()[group_cols + ["n", "Qc_sum"]]
#     return counts
