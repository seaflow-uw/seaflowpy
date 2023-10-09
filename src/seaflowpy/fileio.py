from contextlib import contextmanager
import gzip
import io
import zlib
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import zstandard
from . import errors
from . import particleops
from .seaflowfile import SeaFlowFile

DEFAULT_EVT_DTYPE = np.float32
PARQUET_EVT_DTYPE = np.float32

@contextmanager
def file_open_r(path, fileobj=None):
    """
    Open path or fileobj for reading as a context manager.

    Data read from the return value of this function will come from path or
    preferentially fileobj if provided. If path is provided and ends with '.gz'
    or '.zst' data will be considered gzip or zstandard compressed even if read
    from fileobj. All resources opened by this function (input file handles) or
    open resources passed to this function (fileobj) will be cleaned up by
    context managers.
    The return value of this function should always be used within a 'with'
    block.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    Context manager for file-like object of bytes or strings.
    """
    # zlib is faster than gzip for decompression of EVT data on MacOS, and
    # comparable on Linux.
    path = Path(path)
    if fileobj:
        if path.suffix == '.gz':
            zobj = zlib.decompressobj(wbits=zlib.MAX_WBITS|32)
            data = zobj.decompress(fileobj.read())
            yield io.BytesIO(data)
        elif path.suffix == '.zst':
            dctx = zstandard.ZstdDecompressor()
            with dctx.stream_reader(fileobj) as stream_reader:
                yield stream_reader
        else:
            yield fileobj
    else:
        if path.suffix == '.gz':
            with io.open(path, 'rb') as fileobj:
                zobj = zlib.decompressobj(wbits=zlib.MAX_WBITS|32)
                data = zobj.decompress(fileobj.read())
                yield io.BytesIO(data)
        elif path.suffix == '.zst':
            with io.open(path, 'rb') as fileobj:
                dctx = zstandard.ZstdDecompressor()
                stream_reader = dctx.stream_reader(fileobj)
                yield stream_reader
        else:
            with open(path, 'rb') as fh:
                yield fh


@contextmanager
def file_open_w(path):
    """
    Open path for writing as a context manager.

    If path ends with '.gz' data will gzip compressed. Only the write method of
    the returned object should be used. All resources opened in this function
    (output file handles, gzipping child processes) will be cleaned up by
    context managers. The return value of this function should always be used
    within a 'with' block.

    Parameters
    -----------
    path: str
        File path.

    Returns
    -------
    Context manager for writable file-like object.
    """
    path = Path(path)
    if path.suffix == '.gz':
        with gzip.open(path, mode='wb', compresslevel=9) as fh:
            yield fh
    elif path.suffix == '.zst':
        cctx = zstandard.ZstdCompressor()
        with open(path, 'wb') as fh:
            with cctx.stream_writer(fh) as compressor:
                yield compressor
    else:
        with open(path, 'wb') as fh:
            yield fh


def read_evt_metadata(path):
    path = Path(path)
    if path.suffix == ".parquet":
        with file_open_r(path) as fh:
            return read_evt_parquet_metadata(fh)
    else:
        with file_open_r(path) as fh:
            return read_evt_labview_metadata(fh)


def read_evt_labview_metadata(f):
    """
    Read labview EVT file metadata.

    Parameters
    -----------
    f: File-like object

    Raises
    ------
    errors.FileError

    Returns
    -------
    dict
        { "rowcnt": row count, "colcnt": column count, "version": v1 or v2 }
    """
    bytes_per_col = 2

    try:
        # v1: number of rows of data
        # v2: bytes per row of data, should always be 14 (7 columns * 2 bytes)
        buff1 = f.read(4)
        # v1: number of 2-byte columns per row, should always be 10
        # v2: number of rows of data
        buff2 = f.read(4)
    except (IOError, EOFError, zlib.error) as e:
        raise errors.FileError("File could not be read: {}".format(str(e)))

    if len(buff1) < 4 or len(buff2) < 4:
        raise errors.FileError("File has incomplete leading 32bit numbers")
    num1 = int.from_bytes(buff1, byteorder='little')
    num2 = int.from_bytes(buff2, byteorder='little')
    if num2 == len(particleops.COLUMNS):
        return { "rowcnt": num1, "colcnt": num2, "version": "v1" }
    if num1 == len(particleops.COLUMNS2) * bytes_per_col:
        return { "rowcnt": num2, "colcnt": num1 / bytes_per_col, "version": "v2" }
    raise errors.FileError("File does not have a valid column size number")


def read_evt_parquet_metadata(f):
    """
    Read Parquet EVT metadata.

    Parameters
    -----------
    f: File-like object

    Returns
    -------
    dict
        { "rowcnt": row count, "colcnt": column count, "version": "parquet" }
    """
    metadata = pq.read_metadata(f)
    return {
        "rowcnt": metadata.num_rows,
        "colcnt": metadata.num_columns,
        "version": "parquet"
    }


def read_labview(path, columns=None, fileobj=None, dtype=DEFAULT_EVT_DTYPE):
    """
    Read a labview binary SeaFlow data file.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    columns: list of str, default is None
        Names of columns. If none are given reasonable defaults are used.
    fileobj: io.BytesIO, optional
        Open file object.
    dtype: numpy.dtype, optional
        dtype for numberic columns in EVT dataframe.

    Returns
    -------
    dict of {
        "version": "v1"|"v2",
        "df": SeaFlow event pandas.DataFrame
    }
    """
    try:
        with file_open_r(path, fileobj) as fh:
            counts = read_evt_labview_metadata(fh)
            rowcnt, version = counts["rowcnt"], counts["version"]
            if rowcnt == 0:
                raise errors.FileError("File has no particle data")

            if version == "v1":
                # v1 EVT
                if columns is None:
                    columns = particleops.COLUMNS
                colcnt = len(columns)  # for compat with old labview OPP files
                # Each row has a leading 32-bit int (column count) which can be
                # discarded. We'll account for it by adding 2 to the colcnt
                # (2 extra 16-bit columns)
                colcnt += 2
                expected_bytes = rowcnt * colcnt * 2  # 2 bytes per column
                # Put the leading 32-bit int back in front of the first row
                # since we already read it to get colcnt.
                buff = int(colcnt).to_bytes(4, byteorder='little') + fh.read(int(expected_bytes) - 4)
            elif version == "v2":
                # v2 EVT
                if columns is None:
                    columns = particleops.COLUMNS2
                colcnt = len(columns)  # for compat with old labview OPP files
                # Unlike v1, there are no leading 32-bit ints for colcnt, except
                # for the one we read at the beginning.
                expected_bytes = rowcnt * colcnt * 2  # 2 bytes per column
                buff = fh.read(int(expected_bytes))
            else:
                raise ValueError("invalid version string")

            # Read any extra data at the end of the file for error checking. There
            # shouldn't be any extra data, btw.
            extra_bytes = 0
            while True:
                new_bytes = len(fh.read(8192))
                extra_bytes += new_bytes
                if new_bytes == 0:  # end of file
                    break
    except (IOError, EOFError, zlib.error) as e:
        raise errors.FileError("File could not be read: {}".format(str(e)))

    # Check that file has the expected number of data bytes.
    found_bytes = len(buff) + extra_bytes
    if found_bytes != expected_bytes:
        raise errors.FileError(
            "File has incorrect number of data bytes. Expected %i, saw %i" %
            (expected_bytes, found_bytes)
        )

    events = np.frombuffer(buff, dtype="uint16", count=rowcnt*colcnt)
    # Reshape into a matrix of colcnt columns and one row per particle
    events = np.reshape(events, [rowcnt, colcnt])

    # v1 file, remove leading two columns (32-bit column count int in each row)
    if version == "v1":
        events = np.delete(events, [0, 1], 1)

    # Create a Pandas DataFrame with descriptive column names.
    df = pd.DataFrame(events, columns=columns)

    if not (df.dtypes == dtype).all():
        df = df.astype(dtype)

    return {"version": version, "df": df}


def read_evt_labview(path, fileobj=None, dtype=DEFAULT_EVT_DTYPE):
    """
    Read a raw labview binary SeaFlow data file.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    ----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.
    dtype: numpy.dtype, optional
        dtype for numberic columns in EVT dataframe.

    Returns
    -------
    dict of {
        "version": "v1"|"v2",
        "df": SeaFlow event pandas.DataFrame
    }
    """
    return read_labview(path, columns=None, fileobj=fileobj, dtype=dtype)


def read_evt(path, dtype=DEFAULT_EVT_DTYPE):
    """
    Read EVT file as raw binary, gzipped binary, or reduced Parquet.

    Parameters
    ----------
    path: str
        File path.
    dtype: numpy.dtype, optional
        dtype for numberic columns in EVT dataframe.

    Returns
    -------
    dict of {
        "version": "v1"|"v2"|"parquet",
        "df": SeaFlow event pandas.DataFrame
    }
    """
    path = Path(path)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
        if (df.dtypes != dtype).any():
            df = df.astype(dtype)
        return { "version": "parquet", "df": df }
    else:
        return read_evt_labview(path, dtype=dtype)


def read_filter_params_csv(path):
    """
    Read a filter parameters csv file.

    Parameters
    ----------
    path: str
        Path to filter parameters csv file.

    Returns
    -------
    pandas.DataFrame
        Contents of csv file with "." in column headers replaced with "_".
    """
    defaults = {
        "sep": str(','),
        "na_filter": True,
        "encoding": "utf-8"
    }
    try:
        df = pd.read_csv(path, **defaults)
    except pd.errors.ParserError as e:
        raise errors.FileError("could not parse {} as csv filter paramater file".format(path)) from e
    # Fix column names
    df.columns = [c.replace('.', '_') for c in df.columns]
    # Make sure serial numbers are treated as strings
    # pandas.read_csv can return a dataframe or TextFileReader so disable
    # member method linting.
    # pylint: disable=no-member
    df = df.astype({"instrument": "str"})
    return df


def write_labview(df, path):
    """
    Write SeaFlow event DataFrame as LabView binary file.

    Parameters
    -----------
    df: pandas.DataFrame
        SeaFlow event DataFrame.
    path: str
        Output file path. If this ends with '.gz' data will be gzip compressed.
    """
    # Make sure directory necessary directory tree exists
    Path(path).parent.mkdir(exist_ok=True, parents=True)

    # Open output file
    with file_open_w(path) as fh:
        # Write 32-bit uint particle count header
        header = np.array([len(df.index)], np.uint32)
        fh.write(header.tobytes())
        if len(df.index) > 0:
            # Convert to uint16 before saving
            df = df.astype(np.uint16)

            # Add leading 4 bytes to match LabViews binary format
            zeros = np.zeros([len(df.index), 1], dtype=np.uint16)
            tens = np.copy(zeros)
            tens.fill(10)
            df.insert(0, "tens", tens)
            df.insert(1, "zeros", zeros)

            # Write particle data
            fh.write(df.values.tobytes())


def write_evt_labview(df, path, outdir, gz=True):
    """
    Write a raw SeaFlow event DataFrame as LabView binary file.

    Parameters
    -----------
    df: pandas.DataFrame
        SeaFlow raw event DataFrame.
    path: str
        File name. This will be converted into a standard SeaFlow file ID and
        will be used to construct the final output file path within outdir. The
        final file name will a ".gz" extension if gz is True.
    outdir: str
        Output directory. This function will create day of year subdirectories
        for EVT binary files.
    gz: bool, default True
        Gzip compress?
    """
    if df is None:
        return

    sfile = SeaFlowFile(path)
    outpath = Path(outdir) / sfile.file_id
    if gz:
        outpath = Path(str(outpath) + ".gz")
    # Only keep columns we intend to write to file
    write_labview(df[particleops.COLUMNS], outpath)


def write_opp_parquet(opp_dfs, date, window_size, outdir):
    """
    Write an OPP Parquet file.

    Use snappy compression.

    Parameters
    -----------
    opp_dfs: pandas.DataFrame
        SeaFlow focused particle DataFrames with file_id, date, and index reflecting
        positions in original EVT DataFrames.
    date: pandas.Timestamp or datetime.datetime object
        Start timestamp for data in df.
    window_size: pandas offset alias for time window covered by this file. Time
        covered by this file is date + time_window.
    outdir: str
        Output directory.
    """
    if not opp_dfs:
        return

    # Make sure directory necessary directory tree exists
    outdir = Path(outdir)
    outdir.mkdir(exist_ok=True, parents=True)
    outpath = outdir / (date.isoformat().replace(":", "-") + f".{window_size}.opp.parquet")
    df = pd.concat(opp_dfs, ignore_index=True)
    # Linearize data columns
    df = particleops.linearize_particles(df, columns=["D1", "D2", "fsc_small", "pe", "chl_small"])
    # Only keep columns we intend to write to file, reorder
    columns = [
        "date",
        "file_id",
        "D1",
        "D2",
        "fsc_small",
        "pe",
        "chl_small",
        "q2.5",
        "q50",
        "q97.5",
        "filter_id"
    ]
    df = df[columns]
    # Check for an existing file. Merge, overwriting matching existing entries.
    try:
        old_df = pd.read_parquet(outpath)
    except FileNotFoundError:
        pass
    else:
        if not all(old_df.columns == df.columns):
            raise ValueError("existing OPP parquet file has incompatible column names")
        new_files = list(df["file_id"].unique())
        old_df = old_df[~old_df["file_id"].isin(new_files)]  # drop rows in old_df that are in new data
        df = pd.concat([old_df, df], ignore_index=True)
        df.sort_values(by="file_id", kind="mergesort", inplace=True)  # mergesort is stable

    # Make sure file_id and filter_id are categorical columns
    if df["file_id"].dtype.name != "category":
        df["file_id"] = df["file_id"].astype("category")
    if df["filter_id"].dtype.name != "category":
        df["filter_id"] = df["filter_id"].astype("category")

    # Write parquet
    df.to_parquet(outpath, compression="snappy", index=False, engine="pyarrow")


def binary_to_parquet(infile, outfile):
    df = read_evt(infile, dtype=PARQUET_EVT_DTYPE)["df"][particleops.REDUCED_COLUMNS]
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(outfile)


def validate_evt_file(f, checksum=True, cols=None):
    data = {
        "version": "-",
        "err": None,
        "hash": "-",
        "count": 0
    }

    try:
        file_data = read_evt(f)
    except (errors.FileError, IOError) as e:
        data["err"] = str(e)
    else:
        df = file_data["df"]
        data["version"] = file_data["version"]
        data["count"] = len(df.index)
        if checksum:
            if cols:
                try:
                    data["hash"] = joblib.hash(df[cols].reset_index(drop=True))
                except KeyError as e:
                    data["err"] = str(e)
            else:
                data["hash"] = joblib.hash(df.reset_index(drop=True))

    return pd.Series(data)
