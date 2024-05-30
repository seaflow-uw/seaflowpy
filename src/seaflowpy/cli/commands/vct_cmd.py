import pathlib

import click
from joblib import Parallel, delayed
import pandas as pd
from tqdm import tqdm

from seaflowpy import db
from seaflowpy import util
from seaflowpy import vct



@click.group()
def vct_cmd():
    """VCT file subcommand."""
    pass

@vct_cmd.command("curate")
@click.option("-d", "--db", "dbpath",
    required=True, metavar="FILE", type=click.Path(dir_okay=False, readable=True),
    help="Popcycle SQLite3 db file with populated outlier and sfl tables.")
@click.option("--outdir",
    type=click.Path(path_type=pathlib.Path, file_okay=False, writable=True), required=True,
    help="""Output path for parquet file with subsampled event data.""")
@click.option("-p", "--process-count", default=1, show_default=True, metavar="N", type=int,
    help="Number of processes to use")
@click.option("--quantile",
    type=click.Choice(["2.5", "50", "97.5"]), default="50", show_default=True,
    help="""Filtering quantile to choose.""")
@click.option("--refracs", "refracs_path",
    type=click.Path(path_type=pathlib.Path, dir_okay=False, readable=True), required=True,
    help="""CSV file with population-specific refractive index choices.""")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def curate_vct_cmd(dbpath, outdir, process_count, quantile, refracs_path, files):
    """Curate VCT files by quantile, population refractive indices, and ignored dates"""
    if files:
        files = util.expand_file_list(files)
        vct_paths = sorted([pathlib.Path(f) for f in files if f.endswith(".vct.parquet")])
        refracs = pd.read_csv(refracs_path)
        outliers = db.get_outliers_with_dates(dbpath)
        ignore_dates = outliers[outliers["flag"] != 0]["date"].to_list()
        parallel = Parallel(return_as="generator_unordered", n_jobs=max(1, process_count))
        with tqdm(desc="files", total=len(files)) as bar:
            for res in parallel(delayed(curate_vct_file)(f, refracs, quantile, ignore_dates, outdir) for f in vct_paths):
                bar.update(1)
                bar.set_description(res)

def curate_vct_file(
    vct_path: pathlib.Path, refracs: pd.DataFrame, quantile: str,
    ignore_dates: list[pd.Timestamp], outdir: pathlib.Path
):
    cols = [
        "date",
        f"q{quantile}",
        f"pop_q{quantile}",
        "fsc_small",
        "chl_small",
        "pe",
        f"diam_lwr_q{quantile}",
        f"diam_mid_q{quantile}",
        f"diam_upr_q{quantile}",
        f"Qc_lwr_q{quantile}",
        f"Qc_mid_q{quantile}",
        f"Qc_upr_q{quantile}"
    ]
    df = pd.read_parquet(vct_path, columns=cols)
    df = vct.curate(df, refracs, quantile, ignore_dates)
    if len(df):
        outdir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(outdir / vct_path.name)
    return vct_path.name
