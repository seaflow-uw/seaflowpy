import gzip
import io
import numpy as np
import os
import pandas as pd
from . import errors
from . import particleops
from .seaflowfile import SeaFlowFile
from . import util


def file_open_r(path, fileobj=None):
    """
    Open path for reading.

    Return a file handle for reading from path or preferentially fileobj if
    provided. If path is provided and ends with '.gz' data will be considered
    gzip compressed even if read from fileobj.

    Parameters
    -----------
    path: str
        File path.
    fileobj: io.BytesIO, optional
        Open file object.

    Returns
    -------
    File object of Bytes.
    """
    if fileobj:
        if path.endswith('.gz'):
            fh = gzip.GzipFile(fileobj=fileobj)
        else:
            fh = fileobj
    else:
        if path.endswith('.gz'):
            fh = gzip.GzipFile(path)
        else:
            fh = io.open(path, 'rb')
    return fh


def file_open_w(path):
    """
    Open path for writing.

    Return a file handle for writing to path. If path ends with '.gz' data will
    gzip compressed.

    Parameters
    -----------
    path: str
        File path.

    Returns
    -------
    Writable file-like object.
    """
    if path.endswith('.gz'):
        fh = gzip.GzipFile(path, mode='wb')
    else:
        fh = io.open(path, 'wb')
    return fh


def read_labview(path, fileobj=None):
    """
    Read a labview binary SeaFlow data file.

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
    fh = file_open_r(path, fileobj)

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

    # Read the rest of the data. Each particle has 12 unsigned
    # 16-bit ints in a row.
    expected_bytes = rowcnt * 12 * 2  # rowcnt * 12 columns * 2 bytes
    # must cast to int here because while BufferedIOReader objects
    # returned from io.open(path, "rb") will accept a numpy.int64 type,
    # io.BytesIO objects will not accept this type and will only accept
    # vanilla int types. This is true for Python 3, not for Python 2.
    buff = fh.read(int(expected_bytes))
    if len(buff) != expected_bytes:
        raise errors.FileError(
            "File has incorrect number of data bytes. Expected %i, saw %i" %
            (expected_bytes, len(buff))
        )
    events = np.frombuffer(buff, dtype="uint16", count=rowcnt*12)
    # Reshape into a matrix of 12 columns and one row per particle
    events = np.reshape(events, [rowcnt, 12])
    # Create a Pandas DataFrame with descriptive column names.
    #
    # The first two uint16s [0,10] from start of each row are left out.
    # These ints are an idiosyncrasy of LabVIEW's binary output format.
    # I believe they're supposed to serve as EOL signals (NULL,
    # linefeed in ASCII), but because the last line doesn't have them
    # it's easier to treat them as leading ints on each line after the
    # header.
    df = pd.DataFrame(np.delete(events, [0, 1], 1), columns=particleops.columns)

    # Convert to float64
    df = df.astype(np.float64)

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

    with file_open_w(path) as fh:
        # Create 32-bit uint particle count header
        header = np.array([len(df.index)], np.uint32)
        # Write 32-bit uint particle count header
        fh.write(header.tobytes())

        if len(df.index) > 0:
            # Only keep labview EVT file format columns
            df = df[particleops.columns]
            # Convert back to original type
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
    sfile = SeaFlowFile(path)
    outpath = os.path.join(outdir, sfile.file_id)
    if gz:
        outpath = outpath + ".gz"
    write_labview(df, outpath)


def write_opp_labview(df, path, outdir, gz=True, require_all=True):
    if df is None:
        return

    sfile = SeaFlowFile(path)

    # Return early if any quantiles got completely filtered out
    write_flag = True
    if require_all:
        for q_col, q, q_str, q_df in particleops.quantiles_in_df(df):
            write_flag = write_flag & q_df[q_col].any()
    if write_flag:
        # Write OPP for each quantile in dataframe. Focused particle flag column
        # for each quantile are in columns "q<quantile>" e.g. q2.5 for the 2.5
        # quantile.
        for q_col, q, q_str, q_df in particleops.quantiles_in_df(df):
            # Make sure to run the quantile through util.quantile_str to
            # normalize e.g. 50.0 to 50 before placing on filesystem.
            outpath = os.path.join(outdir, q_str, sfile.file_id + ".opp")
            if gz:
                outpath = outpath + ".gz"
            write_labview(q_df, outpath)
