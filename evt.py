import pandas as pd
import numpy as np


def read_evt(filename):
  """Read an EVT binary file and return a pandas DataFrame"""
    colnames = ["time", "pulse_width", "D1", "D2",
                "fsc_small", "fsc_perp", "fsc_big",
                "pe", "chl_small", "chl_big"]
    with open(filename) as fh:
        rowcnt = np.fromfile(fh, dtype="uint32", count=1)
        particles = np.fromfile(fh, dtype="uint16", count=rowcnt*12)
        particles = np.reshape(particles, [rowcnt, 12])
        df = pd.DataFrame(np.delete(particles, [0,1], 1), columns=colnames)
        df = df.astype("int32")
    return df


def filter_evt(orig_evt, notch=1, width=0.5, slope=1):
    """Filter EVT data.

    Args:
        orig_evt: pandas DataFrame of EVT data
        notch: filter notch (default 1)
        width: filter width (default 0.5)
        slope: filter slope (default 1)

    Returns:
        A pandas DataFrame of in focus particles
    """
    # Only keep particles detected by fsc_small, D1, and D2
    detected = (orig_evt["fsc_small"] > 1) & (orig_evt["D1"] > 1) & (orig_evt["D2"] > 1)
    # Not sure if copy() is needed, but want to make sure we don't change original data
    evt = orig_evt[detected].copy()

    # Only keep particles that have not saturated D1 or D2
    maxD1D2 = evt[["D1", "D2"]].max().max()
    evt = evt[(evt["D1"] < maxD1D2) & (evt["D2"] < maxD1D2)]

    # Correct for D1 D2 sensitivity difference
    origin_data = evt[(evt["D1"] > 5000) | (evt["D2"] > 5000)]
    origin = (origin_data["D2"] - origin_data["D1"]).median()
    if (origin > 0):
        evt["D1"] = evt["D1"] + origin
    elif (origin < 0):
        evt["D2"] = evt["D2"] - origin

    # Filter aligned particles (D1 = D2), with correction for D1 D2 sensitivity difference
    alignedD1 = evt["D1"] < (evt["D2"] * slope + (width * 10**4))
    alignedD2 = evt["D2"] < (evt["D1"] * slope + (width * 10**4))
    aligned = evt[alignedD1 & alignedD2]

    oppD1 = aligned["D1"] / aligned["fsc_small"] < notch
    oppD2 = aligned["D2"] / aligned["fsc_small"] < notch
    opp = aligned[oppD1 & oppD2].copy()

    # Back to original D1 and D2 values
    if (origin > 0):
        opp["D1"] = opp["D1"] - origin
    elif (origin < 0):
        opp["D2"] = opp["D2"] + origin

    return opp
