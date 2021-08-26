from contextlib import contextmanager
import gzip
import io
import os
import zlib
import numpy as np
import pandas as pd
from . import errors
from . import particleops
from .seaflowfile import SeaFlowFile
from . import util


@contextmanager
def file_open_r(path, fileobj=None, as_text=False):
    """
    Open path or fileobj for reading as a context manager.

    Data read from the return value of this function will come from path or
    preferentially fileobj if provided. If path is provided and ends with '.gz'
    data will be considered gzip compressed even if read from fileobj. All
    resources opened by this function (input file handles) or open resources
    passed to this function (fileobj) will be cleaned up by context managers.
    The return value of this function should always be used within a 'with'
    block.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.
    as_text: bool, default False
        Return a file-like object of strings rather than bytes

    Returns
    -------
    Context manager for file-like object of bytes or strings.
    """
    # zlib is faster than gzip for decompression of EVT data on MacOS, and
    # comparable on Linux.
    if fileobj:
        if path.endswith('.gz'):
            gzdata = fileobj.read()
            zobj = zlib.decompressobj(wbits=zlib.MAX_WBITS|32)
            data = zobj.decompress(gzdata)
            if not as_text:
                yield io.BytesIO(data)
            else:
                yield io.TextIOWrapper(io.BytesIO(data))
        else:
            if not as_text:
                yield fileobj
            else:
                yield io.TextIOWrapper(fileobj)
    else:
        if path.endswith('.gz'):
            with io.open(path, 'rb') as fileobj:
                gzdata = fileobj.read()
                zobj = zlib.decompressobj(wbits=zlib.MAX_WBITS|32)
                data = zobj.decompress(gzdata)
                if not as_text:
                    yield io.BytesIO(data)
                else:
                    yield io.TextIOWrapper(io.BytesIO(data))
        else:
            if not as_text:
                with io.open(path, 'rb') as fh:
                    yield fh
            else:
                with io.open(path, 'r') as fh:
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
    if path.endswith('.gz'):
        with gzip.open(path, mode='wb', compresslevel=9) as fh:
            yield fh
    else:
        with io.open(path, 'wb') as fh:
            yield fh


def read_labview(path, columns, fileobj=None):
    """
    Read a labview binary SeaFlow data file.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    columns: list of str
        Names of columns. Also represents how many columns there are.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    pandas.DataFrame
        SeaFlow event DataFrame as numpy.float64 values.
    """
    colcnt = len(columns) + 2  # 2 leading column per row

    try:
        with file_open_r(path, fileobj) as fh:
            # Particle count (rows of data) is stored in an initial 32-bit
            # unsigned int
            buff = fh.read(4)
            if len(buff) == 0:
                raise errors.FileError("File is empty")
            if len(buff) != 4:
                raise errors.FileError("File has invalid particle count header")
            rowcnt = np.frombuffer(buff, dtype="uint32", count=1)[0]
            if rowcnt == 0:
                raise errors.FileError("File has no particle data")

            # Read the rest of the data. Each particle has colcnt unsigned
            # 16-bit ints in a row.
            expected_bytes = rowcnt * colcnt * 2  # rowcnt * colcnt columns * 2 bytes
            # must cast to int here because while BufferedIOReader objects
            # returned from io.open(path, "rb") will accept a numpy.int64 type,
            # io.BytesIO objects will not accept this type and will only accept
            # vanilla int types. This is true for Python 3, not for Python 2.
            buff = fh.read(int(expected_bytes))

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
    # Create a Pandas DataFrame with descriptive column names.
    #
    # The first two uint16s [0,10] from start of each row are left out.
    # These ints are an idiosyncrasy of LabVIEW's binary output format.
    # I believe they're supposed to serve as EOL signals (NULL,
    # linefeed in ASCII), but because the last line doesn't have them
    # it's easier to treat them as leading ints on each line after the
    # header.
    df = pd.DataFrame(np.delete(events, [0, 1], 1), columns=columns)
    return df


def read_labview_row_count(path, fileobj=None):
    """
    Get the row count of a labview binary SeaFlow data file.

    Only a small amount of data from the beginning of the file will be read to
    get the reported row count from the file header. This should be a much
    faster method of getting row count than reading the whole file. Data will
    be read from the file at the provided path or preferentially from fileobj
    if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    int
        Number of rows reported in the labview file header (first uint32).
    """
    if path.endswith('.gz'):
        with io.open(path, 'rb') as fh:
            fileobj = io.BytesIO(fh.read(512))  # read enough to get first 4 uncompressed bytes
    with file_open_r(path, fileobj) as fh:
        # Particle count (rows of data) is stored in an initial 32-bit
        # unsigned int
        try:
            buff = fh.read(4)
        except (IOError, EOFError) as e:
            raise errors.FileError("File could not be read: {}".format(str(e)))
        if len(buff) == 0:
            raise errors.FileError("File is empty")
        if len(buff) != 4:
            raise errors.FileError("File has invalid particle count header")
        rowcnt = np.frombuffer(buff, dtype="uint32", count=1)[0]
    return rowcnt


def read_evt_labview(path, fileobj=None):
    """
    Read a raw labview binary SeaFlow data file.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    pandas.DataFrame
        SeaFlow event DataFrame as numpy.float64 values.
    """
    return read_labview(path, particleops.COLUMNS, fileobj).astype(np.float64)


def read_opp_labview(path, fileobj=None):
    """
    Read an OPP labview binary SeaFlow data file.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    pandas.DataFrame
        SeaFlow OPP DataFrame as numpy.float64 values with quantile flag
        columns.
    """
    df = read_labview(path, particleops.COLUMNS + ["bitflags"], fileobj)
    df[particleops.COLUMNS] = df[particleops.COLUMNS].astype(np.float64)
    df["noise"] = False  # we know there are no noise events in OPP data
    df["saturated"] = False  # we know there are no saturated events in OPP data
    df = particleops.decode_bit_flags(df)
    return df


def read_vct_csv(path, fileobj=None):
    """
    Read a VCT space-separated CSV SeaFlow data file for one quantile.

    Data will be read from the file at the provided path or preferentially from
    fileobj if provided. If path is provided and ends with '.gz' data will be
    considered gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    pandas.DataFrame
        SeaFlow VCT DataFrame for one quantile as numpy.float64 values plus
        one text column for population labels.
    """
    with file_open_r(path, fileobj) as fh:
        df = pd.read_csv(fh, sep=" ", names=["diam_lwr", "Qc_lwr", "diam_mid", "Qc_mid", "diam_upr", "Qc_upr", "pop"])
        return df


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
    except pd.errors.ParserError:
        raise errors.FileError("could not parse {} as csv filter paramater file".format(path))
    # Fix column names
    df.columns = [c.replace('.', '_') for c in df.columns]
    # Make sure serial numbers are treated as strings
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
    util.mkdir_p(os.path.dirname(path))

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
    outpath = os.path.join(outdir, sfile.file_id)
    if gz:
        outpath = outpath + ".gz"
    # Only keep columns we intend to write to file
    write_labview(df[particleops.COLUMNS], outpath)


def write_opp_labview(df, path, outdir, gz=True, require_all=True):
    """
    Write an OPP SeaFlow event DataFrame as LabView binary file.

    Quantile flags will be encoded as a final bit flag column. The noise column
    will be dropped.

    Parameters
    -----------
    df: pandas.DataFrame
        SeaFlow focused particle DataFrame.
    path: str
        File name. This will be converted into a standard SeaFlow file ID and
        will be used to construct the final output file path within outdir. The
        final file name will also have an ".opp" file extension, plus ".gz"
        extension if gz is True.
    outdir: str
        Output directory. This function will create day of year subdirectories
        for EVT binary files.
    gz: bool, default True
        Gzip compress?
    require_all: bool, default True
        If true all an output file will only be created if there is focused
        particle data for all quantiles.
    """
    if df is None:
        return

    # Return early if any quantiles got completely filtered out
    write_flag = True
    if require_all:
        for q_col, _q, _q_str, q_df in particleops.quantiles_in_df(df):
            write_flag = write_flag & q_df[q_col].any()

    if write_flag:
        # Attach a bit flag column to encode all the per-quantile focused
        # particle flags.
        df = particleops.encode_bit_flags(df.copy())

        sfile = SeaFlowFile(path)
        outpath = os.path.join(outdir, sfile.file_id + ".opp")
        if gz:
            outpath = outpath + ".gz"
        # Only keep columns we intend to write to file
        write_labview(df[particleops.COLUMNS + ["bitflags"]], outpath)


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
    util.mkdir_p(outdir)
    outpath = os.path.join(outdir, date.isoformat().replace(":", "-")) + f".{window_size}.opp.parquet"
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
