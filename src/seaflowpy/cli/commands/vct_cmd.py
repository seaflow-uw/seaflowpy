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
    metavar="FILE", type=click.Path(dir_okay=False, readable=True),
    help="Popcycle SQLite3 db file with populated outlier and sfl tables.")
@click.option("-o", "--outdir",
    type=click.Path(path_type=pathlib.Path, file_okay=False, writable=True), required=True,
    help="""Output path for parquet file with subsampled event data.""")
@click.option("-p", "--process-count", default=1, show_default=True, metavar="N", type=int,
    help="Number of processes to use")
@click.option("-q", "--quantile",
    type=click.Choice(["2.5", "50", "97.5"]), default="50", show_default=True,
    help="""Filtering quantile to choose.""")
@click.option("-r", "--refracs", "refracs_path",
    type=click.Path(path_type=pathlib.Path, dir_okay=False, readable=True),
    help="""CSV file with population-specific refractive index choices.""")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
def curate_vct_cmd(dbpath, outdir, process_count, quantile, refracs_path, files):
    """Curate VCT files by quantile, population refractive indices, and ignored dates"""
    if files:
        files = util.expand_file_list(files)
        vct_paths = sorted([pathlib.Path(f) for f in files if f.endswith(".vct.parquet")])
        if refracs_path:
            refracs = pd.read_csv(refracs_path)
        else:
            refracs = None
        if dbpath:
            outliers = db.get_outliers_with_dates(dbpath)
            ignore_dates = outliers[outliers["flag"] != 0]["date"].to_list()
        else:
            ignore_dates = None
        parallel = Parallel(return_as="generator_unordered", n_jobs=max(1, process_count))
        with tqdm(desc="files", total=len(files)) as bar:
            for res in parallel(delayed(curate_vct_file)(f, quantile, refracs, ignore_dates, outdir) for f in vct_paths):
                bar.update(1)
                bar.set_description(res)

def curate_vct_file(
    vct_path: pathlib.Path, quantile: str, refracs: pd.DataFrame | None, 
    ignore_dates: list[pd.Timestamp] | None, outdir: pathlib.Path
):
    df = vct.curate(vct_path, quantile, refracs, ignore_dates)
    if len(df):
        outdir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(outdir / vct_path.name, index = False)
    return vct_path.name
