from pathlib import Path

import click
import numpy as np
import seaflowpy as sfp

@click.command()
@click.argument('v1dir', nargs=1)
@click.argument('v2dir', nargs=1)
def cli(v1dir, v2dir):
    """Convert v1 EVT files to v2"""
    v1files = sfp.seaflowfile.find_evt_files(v1dir)
    v2files = []
    for f in v1files:
        sf = sfp.seaflowfile.SeaFlowFile(f)
        v2files.append(Path(v2dir) / sf.dayofyear / sf.filename_orig)
    for v1, v2 in zip(v1files, v2files):
        convert_v1_to_v2(v1, v2)


def convert_v1_to_v2(v1path, v2path):
    print(f"converting {v1path} to {v2path}")
    Path(v2path).parent.mkdir(exist_ok=True, parents=True)
    try:
        df1 = sfp.fileio.read_evt_labview(v1path)["df"]
    except sfp.errors.FileError as e:
        print(e)
        return
    df2 = df1[["pulse_width", "chl_small", "D1", "D2", "fsc_small", "pe", "chl_big"]]
    df2 = df2.rename(columns={"chl_big": "evt_rate"})
    with sfp.fileio.file_open_w(v2path) as fh:
        colcnt = np.array([len(df2.columns)], np.uint32)
        fh.write((colcnt * 2).tobytes())
        rowcnt = np.array([len(df2.index)], np.uint32)
        fh.write(rowcnt.tobytes())
        df2 = df2.astype(np.uint16)
        fh.write(df2.values.tobytes())

if __name__ == "__main__":
    cli()
