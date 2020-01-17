import os
import pathlib
import hdbscan
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from kern_smooth import densCols
from scipy.spatial import ConvexHull
from seaflowpy import errors
from seaflowpy import fileio
from seaflowpy import particleops



def cluster(df, columns, min_cluster_frac):
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

    Returns
    -------
    dict
        A dictionary of clustering results.

    Raises:
    -------
    TooManyClustersError if more than one cluster is found.
    NoClusterError if no clusters are found.
    """
    min_cluster_size = int(len(df) * min_cluster_frac)
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        allow_single_cluster=True,
        cluster_selection_method="eom").fit(df[columns].values)
    nclust = len(set(clusterer.labels_))
    if nclust > 2:
        raise errors.TooManyClustersError(f"too many {columns} clusters found: {nclust}")
    if nclust == 0:
        raise errors.NoClusterError(f"no {columns} clusters found")

    # Get all points inside expanded cluster boundaries
    idx = [i for i, x in enumerate(clusterer.labels_) if x >= 0]
    points = df.iloc[idx][columns].values
    center = centroid(points) # x, y center of the cluster
    hull = ConvexHull(points)
    # hull.vertices contains row indexes of points that make up convex hull
    # hull_points is a 2d array of x, y points that make up the convex hull
    hull_points = points[hull.vertices, :]

    return {
        "df": df,  # input data frame for DBSCAN
        "columns": columns, # columns of df used to cluster
        "indices": idx, # indices into df of clustered points
        "points": points, # 2d array of points in cluster
        "center": center, # center of cluster points
        "hull_points": hull_points, # (x, y) of convex hull points
        "clusterer": clusterer
    }


def quantiles(a):
    """Return tuple of 3 quantiles of a (.25, .5, .75)."""
    return (np.quantile(a, .25), np.quantile(a, .5), np.quantile(a, .75))


def params2ip(params):
    """
    Convert filtering parameters to inflection point data frame.
    """
    cmod = {"beads_fsc_small": "fsc_small", "beads_D1": "D1", "beads_D2": "D2"}
    ip = params.rename(columns=cmod)[["quantile", "fsc_small", "D1", "D2"]]
    ip = ip.sort_values(by=["quantile"])
    ip = ip.reset_index(drop=True)
    return ip


def ip2params(ip, serial, width=5000):
    """
    Convert inflection point dataframe to filtering params.

    Parameter
    ---------
    ip: pandas.DataFrame
        Inflection points for 1 micron beads.
    serial: int
        Instrument serial number.
    width: int, 5000
        Filtering parameter "width", tolerance in D1 D2 equality.

    Returns
    -------
    pandas.DataFrame
        Filtering parameter dataframe ready to be saved to a popcycle database.
    """
    slope_url = "https://raw.githubusercontent.com/armbrustlab/seaflow-virtualcore/master/1.bead_calibration/seaflow_filter_slopes.csv"
    slopes = pd.read_csv(slope_url)
    slopes = slopes.astype({"ins": "str"})

    headers = [
        "quantile", "beads_fsc_small",
        "beads_D1", "beads_D2", "width",
        "notch_small_D1", "notch_small_D2",
        "notch_large_D1", "notch_large_D2",
        "offset_small_D1", "offset_small_D2",
        "offset_large_D1", "offset_large_D2"
    ]

    params = pd.DataFrame(columns=headers)
    qs = [
        {"quant": 2.5, "suffix": "_2.5"},
        {"quant": 50.0, "suffix": ""},
        {"quant": 97.5, "suffix": "_97.5"}
    ]
    for i, q in enumerate(qs):
        suffix = q["suffix"]
        # Small particles
        offset_small_D1 = 0
        offset_small_D2 = 0
        notch_small_D1 = round_((ip.loc[i, "D1"])/ip.loc[i, "fsc_small"], 3)
        notch_small_D2 = round_((ip.loc[i, "D2"])/ip.loc[i, "fsc_small"], 3)

        # Large particles
        notch_large_D1 = round_(slopes.loc[slopes["ins"] == serial, f"notch.large.D1{suffix}"].iloc[0], 3)
        notch_large_D2 = round_(slopes.loc[slopes["ins"] == serial, f"notch.large.D2{suffix}"].iloc[0], 3)
        offset_large_D1 = round_(ip.loc[i, "D1"] - notch_large_D1 * ip.loc[i, "fsc_small"])
        offset_large_D2 = round_(ip.loc[i, "D2"] - notch_large_D2 * ip.loc[i, "fsc_small"])

        row = pd.DataFrame(
            columns=headers,
            data=[[
                q["quant"], int(ip.loc[i, "fsc_small"]),
                int(ip.loc[i, "D1"]), int(ip.loc[i, "D2"]), width,
                notch_small_D1, notch_small_D2,
                notch_large_D1, notch_large_D2,
                offset_small_D1, offset_small_D2,
                offset_large_D1, offset_large_D2
            ]]
        )
        params = params.append(row)

    params = params.reset_index(drop=True)
    return params


def round_(x, prec=0):
    """Return x rounded to prec number of decimals."""
    if prec == 0:
        return int(x)
    return float("{1:.{0}f}".format(prec, x))


def find_beads(bead_evt_path, serial, evt_path=None, radius=None, pe_min=45000,
               min_cluster_frac=0.33):
    """
    Find bead coordinates with DBSCAN clustering.

    Parameters
    ----------
    bead_evt_path: str
        Path to an EVT file to use for identifying beads positions.
    serial: int
        Instrument serial number
    evt_path: str
        Path to an EVT file to use for final filtering plots.
    radius: int
        If not None, radius of circle to use to collect bead particles after
        the cluster has been identified. The circle is centered around the
        centroid of the cluster. For FSC vs D1 or D2 the circle's radius is
        doubled. If radius is None, bead particles are those only within the
        cluster.
    pe_min: int, default 45000
        PE minimum cutoff to use when identifying beads clusters. This number
        should be large enough to eliminate other common large clusters.
    min_cluster_frac: float, default 0.33
        Minimum fraction of points that should be in  the cluster.

    Returns
    -------
    dict
        A dictionary of clustering results. See code :/

    Raises
    ------
    TooManyClustersError if more than one bead cluster is found.
    NoClusterError if no clusters are found.
    """
    bead_evt = fileio.read_evt_labview(bead_evt_path)
    if evt_path:
        evt = fileio.read_evt_labview(evt_path)
    else:
        evt = bead_evt
    opp = particleops.roughfilter(bead_evt)
    opp_top = opp[opp["pe"] > pe_min]

    # ----------------------------
    # Find initial fsc vs pe beads
    # ----------------------------
    columns = ["fsc_small", "pe"]
    initial_source = opp_top # data for clusterer
    final_source = bead_evt  # source of final cluster points
    clust_fsc_pe = cluster(initial_source, columns, min_cluster_frac=min_cluster_frac)
    # Get cluster points
    if radius is not None:
        # Get all points inside expanded cluster boundaries as circle
        idx = points_in_circle(*clust_fsc_pe["center"], radius, final_source[columns].values)
    else:
        # Get all points inside cluster boundaries
        idx = points_in_polygon(clust_fsc_pe["hull_points"], final_source[columns].values)
    pe_df = final_source.iloc[idx]  # beads by pe
    if len(pe_df) == 0:
        raise errors.NoClusterError(f"could not find points for {columns} cluster")
    fsc_q = quantiles(pe_df["fsc_small"].values)

    # ------------------------------------------
    # Find fsc vs D1 beads as subset of pe beads
    # ------------------------------------------
    columns = ["fsc_small", "D1"]
    intial_source = pe_df # data for clusterer
    final_source = pe_df  # source of final cluster points
    clust_fsc_d1 = cluster(intial_source, columns, min_cluster_frac=min_cluster_frac)
    # Get cluster points
    if radius is not None:
        # Get all points inside expanded cluster boundaries as circle
        idx = points_in_circle(*clust_fsc_d1["center"], radius, final_source[columns].values)
    else:
        # Get all points inside cluster boundaries
        idx = points_in_polygon(clust_fsc_d1["hull_points"], final_source[columns].values)
    d1_df = final_source.iloc[idx]  # beads by D1
    if len(d1_df) == 0:
        raise errors.NoClusterError(f"could not find points for {columns} cluster")
    d1_q = quantiles(d1_df["D1"].values)[::-1]  # reverse to match Francois' results

    # ------------------------------------------
    # Find fsc vs D1 beads as subset of pe beads
    # ------------------------------------------
    columns = ["fsc_small", "D2"]
    intial_source = pe_df # data for clusterer
    final_source = pe_df  # source of final cluster points
    clust_fsc_d2 = cluster(intial_source, columns, min_cluster_frac=min_cluster_frac)
    # Get cluster points
    if radius is not None:
        # Get all points inside expanded cluster boundaries as circle
        idx = points_in_circle(*clust_fsc_d2["center"], radius, final_source[columns].values)
    else:
        # Get all points inside cluster boundaries
        idx = points_in_polygon(clust_fsc_d2["hull_points"], final_source[columns].values)
    d2_df = final_source.iloc[idx]  # beads by D2
    if len(d2_df) == 0:
        raise errors.NoClusterError(f"could not find points for {columns} cluster")
    d2_q = quantiles(d2_df["D2"].values)[::-1]  # reverse to match Francois' results

    ip = pd.DataFrame({
        "quantile": [2.5, 50, 97.5],
        "fsc_small": fsc_q,
        "D1": d1_q,
        "D2": d2_q
    })
    params = ip2params(ip, serial)
    particleops.mark_focused(evt, params)
    final_opp = particleops.select_focused(evt)

    return {
        "inflection_point": ip,
        "filter_params": params,
        "cluster_results": {
            "fsc_pe": clust_fsc_pe,
            "fsc_D1": clust_fsc_d1,
            "fsc_D2": clust_fsc_d2
        },
        "df": {
            "bead_evt": bead_evt,
            "evt": evt,
            "rough_opp": opp,
            "final_opp": final_opp,
            "opp_top": opp_top,
            "fsc_pe": pe_df,    # points identified as beads by fsc pe
            "fsc_D1": d1_df,    # points identified as beads by fsc D1
            "fsc_D2": d2_df     # points identified as beads by fsc D2
        },
        "radius": radius,
        "pe_min": pe_min,
        "evt_path": evt_path,
        "bead_evt_path": bead_evt_path
    }


def plot(b, plot_file, otherip=None):
    """
    Create bead finding diagnostic plots.

    Parameters
    ----------
    b: dict
        Results dict returned by find_beads.
    plot_file: str
        Plot file to write to. Any directory prefix will be created if necessary.
    otherip: pandas.DataFrame
         Other inflection point data to be compared against bead finding results.
    """
    plot_dir = os.path.dirname(plot_file)
    plot_dir_path = pathlib.Path(plot_dir)
    plot_dir_path.mkdir(parents=True, exist_ok=True)

    bead_evt = b["df"]["bead_evt"]
    evt = b["df"]["evt"]
    opp = b["df"]["rough_opp"]
    final_opp = b["df"]["final_opp"]
    final_opp = final_opp[final_opp["q50"]]
    final_opp_ratio = len(final_opp) / len(evt)
    bead_evt_fsc = bead_evt[bead_evt["fsc_small"] > 0]
    radius = b["radius"]
    pe_min = b["pe_min"]
    ip = b["inflection_point"]

    nrows, ncols = 3, 3
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols)
    fig.suptitle(f"HDBSCAN for {b['bead_evt_path']}")
    fig.set_size_inches(18.5, 18.5)

    # Set axis limits
    for r in range(nrows):
        for c in range(ncols):
            ax[r, c].set_xlim(0, 2**16)
            ax[r, c].set_ylim(0, 2**16)

    # -------------------------
    # Common plot options
    # -------------------------
    nbin = 200
    npoints = 20000
    dens_opts = {"s": 5, "edgecolors": 'none', "alpha": 0.75, "cmap": plt.get_cmap("viridis")}
    back_opts = {"s": 10, "color": 'orange', "linewidth": 0, "alpha": 0.1}
    linelen = 1500
    linewidth = 2

    # -------------------------
    # EVT fsc > 0 pe
    # -------------------------
    thisax = ax[0, 0]
    thisax.set_title(f'EVT with min chl/pe cutoffs')
    thisax.set_xlabel('FSC > 0')
    thisax.set_ylabel('PE')
    x, y = bead_evt_fsc["fsc_small"].head(npoints), bead_evt_fsc["pe"].head(npoints)
    res = b["cluster_results"]["fsc_pe"] # clustering results
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    legend_handles = []
    legend_handles.append(plot_bead_coords(thisax, ip["fsc_small"], res["center"], "north", linewidth, linelen))
    if otherip is not None:
        legend_handles.append(plot_bead_coords(thisax, otherip["fsc_small"], res["center"], "south", linewidth, linelen))
    thisax.legend(handles=legend_handles)

    # -------------------------
    # Rough OPP fsc pe
    # -------------------------
    thisax = ax[0, 1]
    thisax.set_title('Rough OPP based on min chl/pe EVT')
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('PE')
    x, y = opp["fsc_small"].head(npoints), opp["pe"].head(npoints)
    res = b["cluster_results"]["fsc_pe"] # clustering results
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    legend_handles = []
    legend_handles.append(plot_bead_coords(thisax, ip["fsc_small"], res["center"], "north", linewidth, linelen))
    if otherip is not None:
        legend_handles.append(plot_bead_coords(thisax, otherip["fsc_small"], res["center"], "south", linewidth, linelen))
    thisax.legend(handles=legend_handles)

    # -------------------------
    # Final OPP fsc pe
    # -------------------------
    thisax = ax[0, 2]
    thisax.set_title('Final OPP ratio=({:.4f}'.format(final_opp_ratio))
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('PE')
    x, y = final_opp["fsc_small"].head(npoints), final_opp["pe"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)

    # -------------------------
    # Final OPP fsc chl
    # -------------------------
    thisax = ax[1, 0]
    thisax.set_title('Final OPP ratio=({:.4f}'.format(final_opp_ratio))
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('CHL')
    x, y = final_opp["fsc_small"].head(npoints), final_opp["chl_small"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)

    # -------------------------
    # Final OPP chl pe
    # -------------------------
    thisax = ax[1, 1]
    thisax.set_title('Final OPP ratio=({:.4f}'.format(final_opp_ratio))
    thisax.set_xlabel('CHL')
    thisax.set_ylabel('PE')
    x, y = final_opp["chl_small"].head(npoints), final_opp["pe"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)

    # -------------------------
    # Final OPP D1 D2
    # -------------------------
    thisax = ax[1, 2]
    thisax.set_title('Final OPP ratio=({:.4f}'.format(final_opp_ratio))
    thisax.set_xlabel('D1')
    thisax.set_ylabel('D2')
    x, y = final_opp["D1"].head(npoints), final_opp["D2"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)

    # -------------------------
    # Plot fsc pe cluster
    # -------------------------
    thisax = ax[2, 0]
    thisax.set_xlim(30000, 2**16)
    thisax.set_ylim(30000, 2**16)
    res = b["cluster_results"]["fsc_pe"] # clustering results
    df = res["df"]                       # particles for clustering
    back = bead_evt_fsc                  # all EVT particles for backdrop
    columns = res["columns"]
    thisax.set_title(f'Clustering input (OPP PE > {pe_min})')
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('PE')
    # Rough OPP > pe_min fsc pe with EVT backdrop
    thisax.scatter(back[columns[0]], back[columns[1]], **back_opts)
    x, y = df["fsc_small"].head(npoints), df["pe"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    # Plot convex hull of cluster
    plot_cluster(thisax, res["hull_points"], res["center"], radius)
    legend_handles = []
    legend_handles.append(plot_bead_coords(thisax, ip["fsc_small"], res["center"], "north", linewidth, linelen))
    if otherip is not None:
        legend_handles.append(plot_bead_coords(thisax, otherip["fsc_small"], res["center"], "south", linewidth, linelen))
    thisax.legend(handles=legend_handles)

    # -------------------------
    # Plot fsc D1 cluster
    # -------------------------
    thisax = ax[2, 1]
    thisax.set_xlim(20000, 2**16)
    thisax.set_ylim(20000, 2**16)
    res = b["cluster_results"]["fsc_D1"] # clustering results
    df = res["df"]                       # particles for clustering
    back = bead_evt_fsc                  # all EVT particles for backdrop
    columns = res["columns"]
    thisax.set_title(f'Clustering input EVT')
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('D1')
    # bead particles from previous clustering, with EVT backdrop
    thisax.scatter(back[columns[0]], back[columns[1]], **back_opts)
    x, y = df["fsc_small"].head(npoints), df["D1"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    # Plot convex hull of cluster
    plot_cluster(thisax, res["hull_points"], res["center"], radius)
    legend_handles = []
    legend_handles.append(plot_bead_coords(thisax, ip["D1"], res["center"], "west", linewidth, linelen))
    if otherip is not None:
        legend_handles.append(plot_bead_coords(thisax, otherip["D1"], res["center"], "east", linewidth, linelen))
    thisax.legend(handles=legend_handles)

    # -------------------------
    # Plot fsc D2 cluster
    # -------------------------
    thisax = ax[2, 2]
    thisax.set_xlim(20000, 2**16)
    thisax.set_ylim(20000, 2**16)
    res = b["cluster_results"]["fsc_D2"] # clustering results
    df = res["df"]                       # particles for clustering
    back = bead_evt_fsc                  # all EVT particles for backdrop
    columns = res["columns"]
    thisax.set_title(f'Clustering input EVT')
    thisax.set_xlabel('FSC')
    thisax.set_ylabel('D2')
    # bead particles from previous clustering, with EVT backdrop
    thisax.scatter(back[columns[0]], back[columns[1]], **back_opts)
    x, y = df["fsc_small"].head(npoints), df["D2"].head(npoints)
    plot_densities_densCols(thisax, x, y, nbin=nbin, **dens_opts)
    # Plot convex hull of cluster
    plot_cluster(thisax, res["hull_points"], res["center"], radius)
    legend_handles = []
    legend_handles.append(plot_bead_coords(thisax, ip["D2"], res["center"], "west", linewidth, linelen))
    if otherip is not None:
        legend_handles.append(plot_bead_coords(thisax, otherip["D2"], res["center"], "east", linewidth, linelen))
    thisax.legend(handles=legend_handles)

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
    densities = densCols(x, y, nbin=kwargs["nbin"])
    del kwargs["nbin"]
    ax.scatter(x, y, c=densities, **kwargs)


def plot_cluster(ax, points, center, radius):
    """
    Draw a cluster of points.

    Parameters
    ----------
    ax: matplotlib.axis.Axis
        Axis to draw on.
    points: 2d numpy array
        Points defining boundary of the cluster. Does not need to be closed,
        meaning first point doesn't need to be repeated as last point.
    center: list
        Centroid of cluster as [x, y].
    radius: int
        Radius of a circle used to expand collection of points in cluster. If
        this value is None (no circle was used) nothing will be drawn.
    """
    # Draw the convex hull
    ax.plot(points[:, 0], points[:, 1], 'k-')
    # Plot center of cluster
    centerx, centery = center
    ax.plot(centerx, centery, 'r+')
    # Plot expanded circle
    if radius is not None:
        beads_circle = plt.Circle(center, radius, color='r', fill=False)
        ax.add_artist(beads_circle)


def plot_bead_coords(ax, coords, center, direction, w, l):
    """
    direction is north, south, east, west on a compass to express if the lines
    should represent positions in x (north/south) or y (east/west), and to
    indicate if the lines should be above (north) or below (south) center, or
    to the left (west) or right (east) of center.

    Returns a patch which can be used as a legend handle.
    """
    blue = "#4242f5"
    pink = "#f542bc"
    centerx, centery = center
    median = str(int(coords.iloc[1]))
    if direction == "west":
        lines = [
            [(centerx-l, coords.iloc[0]), (centerx, coords.iloc[0])],
            [(centerx-l, coords.iloc[1]), (centerx, coords.iloc[1])],
            [(centerx-l, coords.iloc[2]), (centerx, coords.iloc[2])]
        ]
        lc = mpl.collections.LineCollection(lines, colors=pink, linewidths=w)
        ax.add_collection(lc)
        return mpl.patches.Patch(color=pink, label="auto median: " + median)
        #ax.text(0.1, 0.15, "auto median: " + median, color=pink, transform=ax.transAxes)
    elif direction == "east":
        lines = [
            [(centerx, coords.iloc[0]), (centerx+l, coords.iloc[0])],
            [(centerx, coords.iloc[1]), (centerx+l, coords.iloc[1])],
            [(centerx, coords.iloc[2]), (centerx+l, coords.iloc[2])]
        ]
        lc = mpl.collections.LineCollection(lines, colors=blue, linewidths=w)
        ax.add_collection(lc)
        return mpl.patches.Patch(color=blue, label="other median: " + median)
        #ax.text(0.1, 0.1, "other median: " + median, color=blue, transform=ax.transAxes)
    elif direction == "north":
        lines = [
            [(coords.iloc[0], centery), (coords.iloc[0], centery+l)],
            [(coords.iloc[1], centery), (coords.iloc[1], centery+l)],
            [(coords.iloc[2], centery), (coords.iloc[2], centery+l)]
        ]
        lc = mpl.collections.LineCollection(lines, colors=pink, linewidths=w)
        ax.add_collection(lc)
        return mpl.patches.Patch(color=pink, label="auto median: " + median)
        #ax.text(0.1, 0.15, "auto median: " + median, color=pink, transform=ax.transAxes)
    elif direction == "south":
        lines = [
            [(coords.iloc[0], centery-l), (coords.iloc[0], centery)],
            [(coords.iloc[1], centery-l), (coords.iloc[1], centery)],
            [(coords.iloc[2], centery-l), (coords.iloc[2], centery)]
        ]
        lc = mpl.collections.LineCollection(lines, colors=blue, linewidths=w)
        ax.add_collection(lc)
        return mpl.patches.Patch(color=blue, label="other median: " + median)
        #ax.text(0.1, 0.1, "other median: " + median, color=blue, transform=ax.transAxes)


def centroid(points):
    """Find the centroid of points in 2d numpy array."""
    length = points.shape[0]
    sum_x = np.sum(points[:, 0])
    sum_y = np.sum(points[:, 1])
    return sum_x/length, sum_y/length


def points_in_circle(x, y, r, points):
    """
    Find indexes of points within a circle. x, y, and r define the circle center
    and radius. points is the 2d numpy array of points to test.
    """
    condition = (((x-points[:, 0])**2) + ((y-points[:, 1])**2)) <= r**2
    return np.where(condition)


def points_in_polygon(poly_points, points):
    """
    Find indexes of points within the the polygon defined by poly_points.
    poly_points and points are 2d numpy arrays defining the bounding polygon and
    the points to test. The polygon defined by points will automatically close,
    meaning the first point doesn't need to be repeated as the last point.
    """
    poly = mpl.path.Path(poly_points, closed=True)
    # np.where returns a 1-item tuple. Get the one item alone to make it clearer
    return np.where(poly.contains_points(points))[0]
