import logging
import os
import pathlib
import hdbscan
import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from kern_smooth import densCols
from matplotlib.collections import LineCollection
from scipy.spatial import ConvexHull  # pylint: disable=no-name-in-module
from seaflowpy import errors
from seaflowpy import fileio
from seaflowpy import particleops
from seaflowpy import seaflowfile


def cluster(df, columns, min_cluster_frac, min_points=50):
    """
    Find a 2d cluster of points in df[columns] with HDBSCAN. Clustering has been
    tuned to work well for SeaFlow bead clusters.

    Parameters
    ----------
    df: pandas.DataFrame
        Input to clusterer.
    columns: list
        List of column names to use for clustering. Should be length 2.
    min_cluster_frac: float
        Minimum fraction of points that should be in  the cluster. Passed to
        HDBSCAN's clusterer as min_cluster_size.
    min_points: int
        Raise errors.ClustererError if input dataframe has fewer than this many rows.

    Returns
    -------
    dict
        A dictionary of clustering results.

    Raises:
    -------
    errors.ClusterError if more than one cluster or no cluster is found or input is too small.
    """
    if len(df) < min_points:
        raise errors.ClusterError(f"< {min_points} to cluster on {columns}")
    min_cluster_size = int(len(df) * min_cluster_frac)
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        allow_single_cluster=True,
        cluster_selection_method="eom",
    ).fit(df[columns].values)
    nclust = len(set(clusterer.labels_))
    if nclust > 2:
        raise errors.ClusterError(f"too many {columns} clusters found: {nclust}")
    if nclust == 0:
        raise errors.ClusterError(f"no {columns} clusters found")

    # Get all points inside expanded cluster boundaries
    idx = [i for i, x in enumerate(clusterer.labels_) if x >= 0]
    if len(idx) < 3:
        raise errors.ClusterError(f"{columns} cluster too small to find convex hull")
    points = df.iloc[idx][columns].values
    try:
        hull = ConvexHull(points)
    except Exception as e:
        if type(e).__name__ == "QhullError":
            raise errors.ClusterError(f"could not find convex hull for {columns}")
        raise e
    # hull.vertices contains row indexes of points that make up convex hull
    # hull_points is a 2d array of x, y points that make up the convex hull
    hull_points = points[hull.vertices, :]

    return {
        "df": df,  # input data frame for DBSCAN
        "columns": columns,  # columns of df used to cluster
        "indices": idx,  # indices into df of clustered points
        "points": points,  # 2d array of points in cluster
        "hull_points": hull_points,  # (x, y) of convex hull points
        "clusterer": clusterer,
    }


def quantiles(a, q_levels):
    """Return tuple of quantiles q_levels for a."""
    qs = [np.quantile(a, q) for q in q_levels]
    return qs


def params2ip(params):
    """
    Convert filtering parameters to inflection point data frame.
    """
    col_rename = {"beads_fsc_small": "fsc_small", "beads_D1": "D1", "beads_D2": "D2"}
    ip = params.rename(columns=col_rename)[["quantile", "fsc_small", "D1", "D2"]]
    ip = ip.sort_values(by=["quantile"])
    ip = ip.reset_index(drop=True)
    return ip


def ip2params(ip, slopes, width=5000):
    """
    Convert inflection point dataframe to filtering params.

    Parameter
    ---------
    ip: pandas.DataFrame
        Inflection points for 1 micron beads.
    slopes: pandas.DataFrame
        Calibration slopes for SeaFlow filtering of one instrument.
    width: int, 5000
        Filtering parameter "width", tolerance in D1 D2 equality.

    Returns
    -------
    pandas.DataFrame
        Filtering parameter dataframe ready to be saved to a popcycle database.
    """
    headers = [
        "quantile",
        "beads_fsc_small",
        "beads_D1",
        "beads_D2",
        "width",
        "notch_small_D1",
        "notch_small_D2",
        "notch_large_D1",
        "notch_large_D2",
        "offset_small_D1",
        "offset_small_D2",
        "offset_large_D1",
        "offset_large_D2",
    ]

    params = pd.DataFrame(columns=headers)
    qs = [
        {"quant": 2.5, "suffix": "_2.5"},
        {"quant": 50.0, "suffix": ""},
        {"quant": 97.5, "suffix": "_97.5"},
    ]
    for i, q in enumerate(qs):
        suffix = q["suffix"]
        # Small particles
        offset_small_D1 = 0
        offset_small_D2 = 0
        notch_small_D1 = round_((ip.loc[i, "D1"]) / ip.loc[i, "fsc_small"], 3)
        notch_small_D2 = round_((ip.loc[i, "D2"]) / ip.loc[i, "fsc_small"], 3)

        # Large particles
        notch_large_D1 = round_(slopes.loc[:, f"notch.large.D1{suffix}"].iloc[0], 3)
        notch_large_D2 = round_(slopes.loc[:, f"notch.large.D2{suffix}"].iloc[0], 3)
        offset_large_D1 = round_(
            ip.loc[i, "D1"] - notch_large_D1 * ip.loc[i, "fsc_small"]
        )
        offset_large_D2 = round_(
            ip.loc[i, "D2"] - notch_large_D2 * ip.loc[i, "fsc_small"]
        )

        row = pd.DataFrame(
            columns=headers,
            data=[
                [
                    q["quant"],
                    int(ip.loc[i, "fsc_small"]),
                    int(ip.loc[i, "D1"]),
                    int(ip.loc[i, "D2"]),
                    width,
                    notch_small_D1,
                    notch_small_D2,
                    notch_large_D1,
                    notch_large_D2,
                    offset_small_D1,
                    offset_small_D2,
                    offset_large_D1,
                    offset_large_D2,
                ]
            ],
        )
        params = params.append(row)

    params = params.reset_index(drop=True)
    return params


def round_(x, prec=0):
    """Return x rounded to prec number of decimals."""
    if prec == 0:
        return int(x)
    return float("{1:.{0}f}".format(prec, x))


def find_beads(evt_df, min_cluster_frac=0.33, min_fsc=40000, min_pe=45000):
    """
    Find bead coordinates with DBSCAN clustering.

    Parameters
    ----------
    evt_df: pandas.DataFrame
        EVT particle data.
    min_cluster_frac: float, default 0.33
        Minimum fraction of points that should be in  the cluster.
    min_fsc: int, default 40000
        FSC minimum cutoff to use when identifying beads clusters. This number
        should be large enough to eliminate other common large clusters.
    min_pe: int, default 45000
        PE minimum cutoff to use when identifying beads clusters. This number
        should be large enough to eliminate other common large clusters.

    Returns
    -------
    dict
        A dictionary of clustering results. See code :/

    Raises
    ------
    errors.ClusterError
    """
    q_levels = (0.25, 0.5, 0.75)
    opp = particleops.roughfilter(evt_df)

    # Min value filter
    # Start with a condition that's True for every row
    min_indexer = opp["fsc_small"] >= 0
    if min_pe:
        min_indexer = min_indexer & (opp["pe"] >= min_pe)
    if min_fsc:
        min_indexer = min_indexer & (opp["fsc_small"] >= min_fsc)
    opp_top = opp[min_indexer]

    msg = ""  # any error message encountered during clustering

    # ----------------------------
    # Find initial fsc vs pe beads
    # ----------------------------
    columns = ["fsc_small", "pe"]
    initial_source = opp_top  # data for clusterer
    final_source = evt_df  # source of final cluster points
    clust_fsc_pe = None
    pe_df = pd.DataFrame()
    fsc_q = np.full(len(q_levels), np.nan)
    pe_q = np.full(len(q_levels), np.nan)
    pe_center = None
    try:
        clust_fsc_pe = cluster(initial_source, columns, min_cluster_frac=min_cluster_frac)
        # Get all points inside cluster boundaries
        idx = points_in_polygon(clust_fsc_pe["hull_points"], final_source[columns].values)
        if len(idx) == 0:
            raise errors.ClusterError(f"could not find points for {columns} cluster")
    except errors.ClusterError as e:
        msg = str(e)
    else:
        pe_df = final_source.iloc[idx]  # beads by pe
        fsc_q = quantiles(pe_df["fsc_small"].values, q_levels)
        pe_q = quantiles(pe_df["pe"].values, q_levels)
        pe_center = centroid(pe_df[columns].values)

    # ------------------------------------------
    # Find fsc vs D1 beads as subset of pe beads
    # ------------------------------------------
    clust_fsc_d1 = None
    d1_df = pd.DataFrame()
    d1_q = np.full(len(q_levels), np.nan)
    d1_center = None
    if not msg:
        columns = ["fsc_small", "D1"]
        intial_source = pe_df  # data for clusterer
        final_source = pe_df  # source of final cluster points
        try:
            clust_fsc_d1 = cluster(intial_source, columns, min_cluster_frac=min_cluster_frac)
            # Get all points inside cluster boundaries
            idx = points_in_polygon(clust_fsc_d1["hull_points"], final_source[columns].values)
            if len(idx) == 0:
                raise errors.ClusterError(f"could not find points for {columns} cluster")
        except errors.ClusterError as e:
            msg = str(e)
        else:
            d1_df = final_source.iloc[idx]  # beads by D1
            d1_q = quantiles(d1_df["D1"].values, q_levels)
            d1_center = centroid(d1_df[columns].values)

    # ------------------------------------------
    # Find fsc vs D2 beads as subset of pe beads
    # ------------------------------------------
    clust_fsc_d2 = None
    d2_df = pd.DataFrame()
    d2_q = np.full(len(q_levels), np.nan)
    d2_center = None
    if not msg:
        columns = ["fsc_small", "D2"]
        intial_source = pe_df  # data for clusterer
        final_source = pe_df  # source of final cluster points
        try:
            clust_fsc_d2 = cluster(intial_source, columns, min_cluster_frac=min_cluster_frac)
            # Get all points inside cluster boundaries
            idx = points_in_polygon(clust_fsc_d2["hull_points"], final_source[columns].values)
            if len(idx) == 0:
                raise errors.ClusterError(f"could not find points for {columns} cluster")
        except errors.ClusterError as e:
            msg = str(e)
        else:
            d2_df = final_source.iloc[idx]  # beads by D2
            d2_q = quantiles(d2_df["D2"].values, q_levels)
            d2_center = centroid(d2_df[columns].values)

    coords_df = pd.DataFrame({
        "fsc_small_1Q": fsc_q[0],
        "fsc_small_2Q": fsc_q[1],
        "fsc_small_3Q": fsc_q[2],
        "fsc_small_IQR": fsc_q[2] - fsc_q[0],
        "fsc_small_count": len(pe_df),
        "pe_1Q": pe_q[0],
        "pe_2Q": pe_q[1],
        "pe_3Q": pe_q[2],
        "pe_IQR": pe_q[2] - pe_q[0],
        "pe_count": len(pe_df),
        "D1_1Q": d1_q[0],
        "D1_2Q": d1_q[1],
        "D1_3Q": d1_q[2],
        "D1_IQR": d1_q[2] - d1_q[0],
        "D1_count": len(d1_df),
        "D2_1Q": d2_q[0],
        "D2_2Q": d2_q[1],
        "D2_3Q": d2_q[2],
        "D2_IQR": d2_q[2] - d2_q[0],
        "D2_count": len(d2_df),
    }, index=[0])

    return {
        "bead_coordinates": coords_df,
        "cluster_results": {
            "fsc_pe": clust_fsc_pe,
            "fsc_D1": clust_fsc_d1,
            "fsc_D2": clust_fsc_d2,
        },
        "df": {
            "evt": evt_df,
            "rough_opp": opp,
            "opp_top": opp_top,
            # These are the actual points that are beads from EVT data
            "fsc_pe": pe_df,  # points identified as beads by fsc pe
            "fsc_D1": d1_df,  # points identified as beads by fsc D1
            "fsc_D2": d2_df,  # points identified as beads by fsc D2
        },
        "centers": {  # median centroids
            "fsc_pe": pe_center,
            "fsc_D1": d1_center,
            "fsc_D2": d2_center,
        },
        "pe_min": min_pe,
        "fsc_min": min_fsc,
        "message": msg,
    }


def plot(b, plot_file, file_id=None, otherip=None):
    """
    Create bead finding diagnostic plots.

    Parameters
    ----------
    b: dict
        Results dict returned by find_beads.
    file_id: str
        File ID of cluster input EVT file.
    plot_file: str
        Plot file to write to. Any directory prefix will be created if necessary.
    otherip: pandas.DataFrame
         Other inflection point data to be compared against bead finding results.
    """
    plot_dir = os.path.dirname(plot_file)
    pathlib.Path(plot_dir).mkdir(parents=True, exist_ok=True)

    coords = b["bead_coordinates"]

    nrows, ncols = 2, 3
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols)

    # Set axis limits
    for r in range(nrows):
        for c in range(ncols):
            ax[r, c].set_xlim(0, 2 ** 16)
            ax[r, c].set_ylim(0, 2 ** 16)

    # -------------------------
    # Common plot options
    # -------------------------
    nbin = 300
    npoints = 10000
    dens_opts = {
        "s": 5,
        "edgecolors": "none",
        "alpha": 0.75,
        "cmap": plt.get_cmap("viridis"),
    }
    back_opts = {"s": 10, "color": "orange", "linewidth": 0, "alpha": 0.1}
    linelen = 1500
    linewidth = 2

    # -------------------------
    # EVT
    # -------------------------
    thisax = ax[0, 0]
    thisax.set_title(f"EVT ({len(b['df']['evt'])} events)")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("PE")
    df = b["df"]["evt"]
    x, y = df["fsc_small"].head(npoints), df["pe"].head(npoints)
    center = b["centers"]["fsc_pe"]
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    plot_cutoffs(b["fsc_min"], b["pe_min"], thisax)
    if coords["fsc_small_count"].sum():
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                pd.Series([coords["fsc_small_1Q"], coords["fsc_small_2Q"], coords["fsc_small_3Q"]]),
                center[0],
                center[1],
                "north",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        if otherip is not None:
            legend_handles.append(
                plot_bead_coords(
                    thisax,
                    otherip["fsc_small"],
                    center[0],
                    center[1],
                    "south",
                    linewidth,
                    linelen,
                )
            )
        thisax.legend(handles=legend_handles)

    # -------------------------
    # EVT fsc chl
    # -------------------------
    thisax = ax[0, 1]
    thisax.set_title(f"EVT ({len(b['df']['evt'])} events)")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("CHL")
    df = b["df"]["evt"]
    x, y = df["fsc_small"].head(npoints), df["chl_small"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    plot_cutoffs(b["fsc_min"], None, thisax)

    if coords["fsc_small_count"].sum():
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                None,
                center[0],
                None,
                "north",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        thisax.legend(handles=legend_handles)

    # -------------------------
    # Rough OPP fsc pe
    # -------------------------
    thisax = ax[0, 2]
    thisax.set_title(f"Rough OPP ({len(b['df']['rough_opp'])} events)")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("PE")
    back = b["df"]["evt"]  # all EVT particles for backdrop
    thisax.scatter(back["fsc_small"].head(npoints), back["pe"].head(npoints), **back_opts)
    df = b["df"]["rough_opp"]
    x, y = df["fsc_small"].head(npoints), df["pe"].head(npoints)
    center = b["centers"]["fsc_pe"]
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    plot_cutoffs(b["fsc_min"], b["pe_min"], thisax)
    if coords["fsc_small_count"].sum():
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                pd.Series([coords["fsc_small_1Q"], coords["fsc_small_2Q"], coords["fsc_small_3Q"]]),
                center[0],
                center[1],
                "north",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        if otherip is not None:
            legend_handles.append(
                plot_bead_coords(
                    thisax,
                    otherip["fsc_small"],
                    center[0],
                    center[1],
                    "south",
                    linewidth,
                    linelen,
                )
            )
        thisax.legend(handles=legend_handles)

    # -------------------------
    # Plot fsc pe cluster
    # -------------------------
    thisax = ax[1, 0]
    thisax.set_xlim(25000, 2 ** 16)
    thisax.set_ylim(25000, 2 ** 16)
    res = b["cluster_results"]["fsc_pe"]  # clustering results
    center = b["centers"]["fsc_pe"]
    back = b["df"]["evt"]  # all EVT particles for backdrop
    thisax.set_title(f"Clustered input OPP (PE > {b['pe_min']}, FSC > {b['fsc_min']})")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("PE")
    # Rough OPP > pe_min fsc pe with EVT backdrop
    thisax.scatter(back["fsc_small"].head(npoints), back["pe"].head(npoints), **back_opts)
    plot_cutoffs(b["fsc_min"], b["pe_min"], thisax)
    df = b["df"]["opp_top"]  # particles for clustering
    x, y = df["fsc_small"].head(npoints), df["pe"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    if res is not None:
        # Plot convex hull of cluster
        plot_cluster(thisax, res["hull_points"], center)
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                pd.Series([coords["fsc_small_1Q"], coords["fsc_small_2Q"], coords["fsc_small_3Q"]]),
                center[0],
                center[1],
                "north",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        if otherip is not None:
            legend_handles.append(
                plot_bead_coords(
                    thisax,
                    otherip["fsc_small"],
                    center[0],
                    center[1],
                    "south",
                    linewidth,
                    linelen,
                )
            )
        thisax.legend(handles=legend_handles)

    # -------------------------
    # Plot fsc D1 cluster
    # -------------------------
    thisax = ax[1, 1]
    thisax.set_xlim(0, 2 ** 16)
    thisax.set_ylim(0, 2 ** 16)
    res = b["cluster_results"]["fsc_D1"]  # clustering results
    center = b["centers"]["fsc_D1"]
    back = b["df"]["evt"]  # all EVT particles for backdrop
    thisax.set_title(f"Clustered input EVT")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("D1")
    # bead particles from previous clustering, with EVT backdrop
    thisax.scatter(back["fsc_small"].head(npoints), back["D1"].head(npoints), **back_opts)
    df = b["df"]["fsc_pe"]  # particles for clustering
    if len(df) > 0:
        x, y = df["fsc_small"].head(npoints), df["D1"].head(npoints)
        plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    if res is not None:
        # Plot convex hull of cluster
        plot_cluster(thisax, res["hull_points"], center)
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                pd.Series([coords["D1_1Q"], coords["D1_2Q"], coords["D1_3Q"]]),
                center[0],
                center[1],
                "west",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        if otherip is not None:
            legend_handles.append(
                plot_bead_coords(
                    thisax,
                    otherip["D1"],
                    center[0],
                    center[1],
                    "east",
                    linewidth,
                    linelen,
                )
            )
        thisax.legend(handles=legend_handles)

    # -------------------------
    # Plot fsc D2 cluster
    # -------------------------
    thisax = ax[1, 2]
    thisax.set_xlim(0, 2 ** 16)
    thisax.set_ylim(0, 2 ** 16)
    res = b["cluster_results"]["fsc_D2"]  # clustering results
    center = b["centers"]["fsc_D2"]
    back = b["df"]["evt"]  # all EVT particles for backdrop
    thisax.set_title(f"Clustered input EVT")
    thisax.set_xlabel("FSC")
    thisax.set_ylabel("D2")
    # bead particles from previous clustering, with EVT backdrop
    thisax.scatter(back["fsc_small"].head(npoints), back["D2"].head(npoints), **back_opts)
    df = b["df"]["fsc_pe"]  # particles for clustering
    if len(df) > 0:
        x, y = df["fsc_small"].head(npoints), df["D2"].head(npoints)
        plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    if res is not None:
        # Plot convex hull of cluster
        plot_cluster(thisax, res["hull_points"], center)
        legend_handles = []
        legend_handles.append(
            plot_bead_coords(
                thisax,
                pd.Series([coords["D2_1Q"], coords["D2_2Q"], coords["D2_3Q"]]),
                center[0],
                center[1],
                "west",
                linewidth,
                linelen,
                median_target=True,
            )
        )
        if otherip is not None:
            legend_handles.append(
                plot_bead_coords(
                    thisax,
                    otherip["D2"],
                    center[0],
                    center[1],
                    "east",
                    linewidth,
                    linelen,
                )
            )
        thisax.legend(handles=legend_handles)

    #fig.suptitle(f"HDBSCAN for {file_id}")
    fig.set_size_inches(14, 14)
    fig.tight_layout()
    fig.savefig(plot_file, dpi=200)
    plt.close(fig)


def plot_densities_densCols(ax, x, y, **kwargs):
    """
    Draw points as scatter plot colored by density.

    Parameters
    ----------
    ax: matplotlib.axis.Axis
        Axis to draw on.
    x, y: Iterables of x and y positions of points to draw

    Keyword Arguments
    -----------------
    nbin: int
        nbin parameter passed to kern_smooth.densCols.

    Other keyword arguments will be passed to matplotlib.axis.Axis.scatter.
    """
    # The line in densCols at this location
    # https://github.com/AlexanderButyaev/kern_smooth/blob/bcf0b6dea616b920102c25db35f7d45870b416a6/kern_smooth/ks.py#L48
    # can produce this error if array length is 1
    #
    # kern_smooth/ks.py:48: RuntimeWarning: invalid value encountered in true_divide
    # cols[select] = colpal / (len(dens) - 1.) if len(dens) > 0 else colpal
    if (len(x) < 2) or (len(y) < 2):
        return
    try:
        colors = densCols(x, y, nbin=kwargs["nbin"])
    except ValueError as e:
        # Might be something like this
        # ValueError: Binning grid too coarse for current (small) bandwidth: consider increasing 'gridsize'
        # Fallback to normal scatterplot
        logging.warning("plotting as single color scatter after error from kern_smooth.densCols: %s", str(e))
        colors = "mediumseagreen"
    del kwargs["nbin"]
    ax.scatter(x, y, c=colors, **kwargs)


def plot_cluster(ax, points, center):
    """
    Draw a cluster of points.

    Parameters
    ----------
    ax: matplotlib.axis.Axis
        Axis to draw on.
    points: 2d numpy array
        Points defining boundary of the cluster. Will be closed in this function
        before plotting if necessary, meaning first point will be repeated as
        last point.
    center: list
        Centroid of cluster as [x, y].
    """
    if len(points) < 3:
        return
    # Draw the convex hull
    # close the polygon
    if points[0, 0] != points[-1, 1]:
        points = np.vstack([points, points[0, :]])
    ax.plot(points[:, 0], points[:, 1], "k-")
    # Plot center of cluster
    centerx, centery = center
    ax.plot(centerx, centery, "r+")


def plot_bead_coords(
    ax, coords, centerx, centery, direction, w, l, median_target=False
):
    """
    direction is north, south, east, west on a compass to express if the lines
    should represent positions in x (north/south) or y (east/west), and to
    indicate if the lines should be above (north) or below (south) center, or
    to the left (west) or right (east) of center. To disable one of the lines
    drawn for the target crosshairs, pass a centerx or centery value of None. To
    disable IQR lines in coords, pass None as value.

    Returns a patch which can be used as a legend handle.
    """
    blue = "#1212eb"
    red = "#eb1212"
    alpha = 0.75
    median = "-"
    target_line_width = w * 0.5  # half of quantile lines
    if direction == "west":
        if coords is not None and centerx is not None:
            lines = [
                [(centerx - l, coords.iloc[0]), (centerx, coords.iloc[0])],
                [(centerx - l, coords.iloc[1]), (centerx, coords.iloc[1])],
                [(centerx - l, coords.iloc[2]), (centerx, coords.iloc[2])],
            ]
            lc = mpl.collections.LineCollection(
                lines, colors=red, linewidths=w, alpha=alpha
            )
            ax.add_collection(lc)
        if median_target:
            if centerx is not None:
                ax.axvline(centerx, color=red, linewidth=target_line_width)
            if centery is not None:
                median = str(centery)
                ax.axhline(centery, color=red, linewidth=target_line_width)
        return mpl.patches.Patch(color=red, label="auto median y: " + median)
    elif direction == "east":
        if coords is not None and centerx is not None:
            lines = [
                [(centerx, coords.iloc[0]), (centerx + l, coords.iloc[0])],
                [(centerx, coords.iloc[1]), (centerx + l, coords.iloc[1])],
                [(centerx, coords.iloc[2]), (centerx + l, coords.iloc[2])],
            ]
            lc = mpl.collections.LineCollection(
                lines, colors=blue, linewidths=w, alpha=alpha
            )
            ax.add_collection(lc)
        if median_target:
            if centerx is not None:
                ax.axvline(centerx, color=red, linewidth=target_line_width)
            if centery is not None:
                median = str(centery)
                ax.axhline(centery, color=red, linewidth=target_line_width)
        return mpl.patches.Patch(color=blue, label="other median y: " + median)
    elif direction == "north":
        if coords is not None and centery is not None:
            lines = [
                [(coords.iloc[0], centery), (coords.iloc[0], centery + l)],
                [(coords.iloc[1], centery), (coords.iloc[1], centery + l)],
                [(coords.iloc[2], centery), (coords.iloc[2], centery + l)],
            ]
            lc = mpl.collections.LineCollection(
                lines, colors=red, linewidths=w, alpha=alpha
            )
            ax.add_collection(lc)
        if median_target:
            if centerx is not None:
                median = str(centerx)
                ax.axvline(centerx, color=red, linewidth=target_line_width)
            if centery is not None:
                ax.axhline(centery, color=red, linewidth=target_line_width)
        return mpl.patches.Patch(color=red, label="auto median x: " + median)
    elif direction == "south":
        if coords is not None and centery is not None:
            lines = [
                [(coords.iloc[0], centery - l), (coords.iloc[0], centery)],
                [(coords.iloc[1], centery - l), (coords.iloc[1], centery)],
                [(coords.iloc[2], centery - l), (coords.iloc[2], centery)],
            ]
            lc = mpl.collections.LineCollection(
                lines, colors=blue, linewidths=w, alpha=alpha
            )
            ax.add_collection(lc)
        if median_target:
            if centerx is not None:
                median = str(centerx)
                ax.axvline(centerx, color=red, linewidth=target_line_width)
            if centery is not None:
                ax.axhline(centery, color=red, linewidth=target_line_width)
        return mpl.patches.Patch(color=blue, label="other median x: " + median)


def plot_cutoffs(xcutoff, ycutoff, ax):
    """
    Plot black lines for bead clustering cutoffs.

    If xcutoff or ycutoff is None it won't be plotted.
    """
    ylow, yhigh = ax.get_ylim()
    xlow, xhigh = ax.get_xlim()

    lines = []
    if xcutoff is not None:
        if ycutoff:
            ylow = ycutoff
        lines.append([[xcutoff, ylow], [xcutoff, yhigh]])
    if ycutoff is not None:
        if xcutoff:
            xlow = xcutoff
        lines.append([[xlow, ycutoff], [xhigh, ycutoff]])
    lc = mpl.collections.LineCollection(lines, colors="black")
    ax.add_collection(lc)


def centroid(points):
    """Find the median centroid of points in 2d numpy array."""
    return np.median(points[:, 0]), np.median(points[:, 1])


def points_in_polygon(poly_points, points):
    """
    Find indexes of points within the the polygon defined by poly_points.
    poly_points and points are 2d numpy arrays defining the bounding polygon and
    the points to test. The polygon defined by points will automatically close,
    meaning the first point doesn't need to be repeated as the last point.
    """
    # close the polygon, may not be necessary but won't hurt
    if len(poly_points) < 2:
        return np.array([], dtype=int)
    if poly_points[0, 0] != poly_points[-1, 1]:
        poly_points = np.vstack([poly_points, poly_points[0, :]])
    poly = mpl.path.Path(poly_points)
    # np.where returns a 1-item tuple. Get the one item alone to make it clearer
    return np.where(poly.contains_points(points))[0]


def aggregate_evt_files(files, dates=None):
    """
    Aggregate EVT file data into one dataframe with dates and file_ids.

    If no EVT files can be read an empty dataframe will be returned.
    """
    channels = ["fsc_small", "chl_small", "pe", "D1", "D2"]
    dfs = []
    for f in files:
        try:
            sff = seaflowfile.SeaFlowFile(f)
            file_id = sff.file_id
        except errors.FileError:
            sff = None
            file_id = None
        if dates:
            try:
                date = dates[file_id]
            except KeyError:
                date = None
        elif sff:
            date = sff.date
        else:
            date = None

        logging.debug("reading %s with date = %s and file_id = %s", f, date, file_id)
        try:
            df = fileio.read_evt_labview(f)[channels]
        except errors.FileError as e:
            logging.warning("error reading EVT file %s: %s", f, e)
            continue
        df["file_id"] = file_id
        df["date"] = date
        dfs.append(df)
    # Use mergesort for a stable sort
    if len(dfs) == 0:
        return pd.DataFrame()
    out_df = pd.concat(dfs, ignore_index=True).sort_values(
        by=["date"], kind="mergesort"
    )
    out_df["file_id"] = out_df["file_id"].astype("category")
    return out_df



def plot_cruise(bead_df, outpath, filter_params_path="", cruise="", iqr=None):
    if filter_params_path:
        filt_df = pd.read_csv(filter_params_path)
        filt_df = filt_df.rename(columns={
            "beads.fsc.small": "fsc_small",
            "beads.D1": "D1",
            "beads.D2": "D2",
        })
        filt_df = filt_df[filt_df["quantile"] == 50]  # only keep 50th quantile
    else:
        filt_df = None

    fig, axs = plt.subplots(nrows=3, ncols=1, gridspec_kw={'height_ratios': [1, 1, 1]}, squeeze=False, figsize=(12, 18))
    fig.suptitle(f"{cruise} bead positions - IQR cutoff (green to red) {iqr}")
    fig.set_size_inches(9, 12)

    for ax, col in zip(axs.flat, ["fsc_small", "D1", "D2"]):
        plot_column(ax, bead_df, col, filt_df, iqr=iqr)

    fig.tight_layout()
    plt.subplots_adjust(top=0.93)
    fig.savefig(outpath, dpi=200)
    plt.close(fig)


def plot_column(ax, bead_df, col, filt_df=None, cruise="", iqr=None):
    bead_df = bead_df.copy()
    bead_df["date"] = mdates.date2num(bead_df["date"])  # for better matplotlib compat
    locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
    formatter = mdates.ConciseDateFormatter(locator)
    ylims = (0, 2**16)
    markersize = 3

    res_n = len(bead_df["resolution"].values.unique())
    if res_n != 1:
        raise ValueError("could not determine bead finding resolution, saw {} values".format(res_n))
    resolution = bead_df["resolution"].values[0]

    if filt_df is not None:
        ax.plot_date(
            bead_df["date"],
            np.repeat(filt_df[col].values[0], len(bead_df["date"])),
            c="orange",
            markersize=markersize-1,
            alpha=0.3,
            label="manual filter params"
        )

    ax.plot_date(
        bead_df["date"],
        bead_df[f"{col}_2Q"],
        c="deepskyblue",
        markersize=5,
        marker=mpl.markers.CARETDOWNBASE,
        alpha=1,
        label=f"by-{resolution}"
    )
    # Plot a vertical line for each bead coord point showing interquartile range
    iqr_minys = bead_df[f"{col}_1Q"]
    iqr_maxys = bead_df[f"{col}_3Q"]
    iqr_xs = bead_df["date"]
    iqr_colors = []
    for i, _ in enumerate(iqr_xs):
        if iqr and bead_df.loc[i, f"{col}_IQR"] > iqr:
            iqr_colors.append(mpl.colors.to_rgba("red"))
        else:
            iqr_colors.append(mpl.colors.to_rgba("green"))
    #iqr_colors = [mpl.colors.to_rgba("green") for _ in iqr_xs]
    lower_points = np.column_stack([iqr_xs, iqr_minys])
    upper_points = np.column_stack([iqr_xs, iqr_maxys])
    segs = np.ndarray([len(iqr_xs), 2, 2])
    segs[:, 0, :] = lower_points
    segs[:, 1, :] = upper_points
    lc = LineCollection(segs, colors=iqr_colors, zorder=50)
    ax.add_collection(lc)

    ax.set_title(col)
    ax.legend()
    ax.xaxis.set_major_formatter(formatter)
    ax.set_ylim(ylims)
