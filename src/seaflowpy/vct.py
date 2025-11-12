from collections.abc import (
    Mapping,
    Sequence
)
from pathlib import Path

import numpy as np
import pandas as pd

def curate(
    vct: pd.DataFrame | str | Path, quantile: str, refracs: pd.DataFrame | str | None=None, 
    ignore_dates: pd.Series | Sequence | None=None
):
    """Select VCT data for one quantile, population refractive indices, and remove ignored dates
    
    This function does not modify the original DataFrame.
    """
    pop_col = f"pop_q{quantile}"
    quant_col = f"q{quantile}"
    cols = ["date", "fsc_small", "chl_small", "pe", pop_col, quant_col]
    if refracs is not None and not isinstance(refracs, str):
        cols = cols + [
            f"diam_lwr_{quant_col}",
            f"diam_mid_{quant_col}",
            f"diam_upr_{quant_col}",
            f"Qc_lwr_{quant_col}",
            f"Qc_mid_{quant_col}",
            f"Qc_upr_{quant_col}"
        ]
    elif refracs is not None and isinstance(refracs, str):
        cols = cols + [
            f"diam_{refracs}_{quant_col}",
            f"Qc_{refracs}_{quant_col}"
        ]
    if not isinstance(vct, pd.DataFrame):
        vct = pd.read_parquet(vct, columns = cols)
    else:
        vct = vct.copy()
    vct = vct.rename(columns={pop_col: "pop"})
    vct = vct[vct[quant_col]]
    if ignore_dates is not None:
        vct = vct.loc[~vct["date"].isin(ignore_dates)]
    if refracs is not None and not isinstance(refracs, str):
        vct["diam"] = np.nan
        vct["Qc"] = np.nan
        for pop in [c for c in refracs.columns if c != "cruise"]:
            idx = vct["pop"] == pop
            if idx.any():
                vct.loc[idx, "diam"] = vct.loc[idx, f"diam_{refracs[pop].values[0]}_q{quantile}"]
                vct.loc[idx, "Qc"] = vct.loc[idx, f"Qc_{refracs[pop].values[0]}_q{quantile}"]
        cols  = ["date", "pop", "fsc_small", "chl_small", "pe", "diam", "Qc"]
        # Check for any diam or Qc NaN values, meaning the population wasn't in refracs
        if vct["diam"].isna().any() or vct["Qc"].isna().any():
            raise ValueError("Some populations in VCT data were not found in refracs DataFrame.")
    elif refracs is not None and isinstance(refracs, str):
        vct["diam"] = vct[f"diam_{refracs}_q{quantile}"]
        vct["Qc"] = vct[f"Qc_{refracs}_q{quantile}"]
        cols  = ["date", "pop", "fsc_small", "chl_small", "pe", "diam", "Qc"]
    else:
        cols  = ["date", "pop", "fsc_small", "chl_small", "pe"]
    return vct[cols]


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

