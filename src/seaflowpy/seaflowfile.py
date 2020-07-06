import os
import re

import pandas as pd
from . import errors
from . import time
from . import util


dayofyear_re = r'^\d{1,4}_\d{1,3}$'
new_path_re = r'^\d{1,4}_\d{1,3}/\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-\d{2}$'
new_file_re = r'^(?P<date>\d{4}-\d{2}-\d{2})T(?P<hours>\d{2})-(?P<minutes>\d{2})-(?P<seconds>\d{2})(?P<tzhours>[+-]\d{2})-(?P<tzminutes>\d{2})$'
old_path_re = r'^\d{1,4}_\d{1,3}/\d+\.evt$'
old_file_re = r'^\d+\.evt$'
evt_file_re = r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-\d{2}(?:\.gz)?$|^\d+\.evt(?:\.gz)?$'
opp_file_re = r'^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-\d{2}\.opp(?:\.gz)?$|^\d+\.evt\.opp(?:\.gz)?$'


class SeaFlowFile:
    """Base class for EVT/OPP/VCT file classes"""

    def __init__(self, path, date=None):
        """
        The date can be set from the date argument for old style filenames that
        don't have timestamps. For new style file names if the date parsed from
        the filename doesn't match the date passed in the contructor a ValueError
        will be raised.
        """
        self.path = path

        parts = parse_path(self.path)
        self.filename = parts["file"]
        self.filename_noext = remove_ext(parts["file"])

        if not (self.is_old_style or self.is_new_style):
            raise errors.FileError("Filename doesn't look like a SeaFlow file")

        if self.is_new_style:
            err = None
            try:
                timestamp = timestamp_from_filename(self.filename_noext)
                self.date = time.parse_date(timestamp)
            except ValueError as e:
                err = e
            if err:
                raise errors.FileError(str(err))
            if date is not None and self.date != date:
                raise ValueError(
                    "parsed date does not match date argument, {} != {}".format(
                        self.date.isoformat(timespec="seconds"),
                        date.isoformat(timespec="seconds")
                    )
                )
        else:
            self.date = date

        # YYYY_dayofyear directory found in file path and parsed
        # from file datestmap
        self.path_dayofyear = parts["dayofyear"]

        # Identifer to match across file types (EVT/OPP/VCT)
        # Should be something like 2014_142/42.evt for old files. Note always
        # .evt even for opp and vct. No .gz.
        # Should be something like 2014_342/2014-12-08T22-53-34+00-00 for new
        # files. Note no extension including .gz.
        # The day of year directory will be based on parsed datestamp in
        # filename when possible, not the given path. The file ID based on
        # the given path is stored in path_file_id.
        if self.is_old_style:
            # path_file_id and file_id are always the same for old-style
            # filenames since we can't parse dates to calculate a day of year
            # directory
            if self.path_dayofyear:
                self.file_id = "{}/{}".format(self.path_dayofyear, self.filename_noext)
            else:
                self.file_id = self.filename_noext
            self.path_file_id = self.file_id
        else:
            self.file_id = "{}/{}".format(self.dayofyear, self.filename_noext)
            if self.path_dayofyear:
                self.path_file_id = "{}/{}".format(self.path_dayofyear, self.filename_noext)
            else:
                self.path_file_id = self.filename_noext

    def __str__(self):
        return "SeaFlowFile: {}, {}".format(self.file_id, self.path)

    @property
    def dayofyear(self):
        """Return day of year based on date."""
        return create_dayofyear_directory(self.date)

    @property
    def isgz(self):
        """Is file gzipped?"""
        return self.path and self.filename.endswith(".gz")

    @property
    def is_old_style(self):
        """Is this old style file? e.g. 2014_185/1.evt."""
        return bool(re.match(old_file_re, self.filename_noext))

    @property
    def is_new_style(self):
        """Is this a new style file? e.g. 2018_082/2018-03-23T00-00-00+00-00.gz"""
        return bool(re.match(new_file_re, self.filename_noext))

    @property
    def is_evt(self):
        """Is this an EVT file?"""
        return bool(re.match(evt_file_re, self.filename))

    @property
    def is_opp(self):
        """Is this an OPP file?"""
        return bool(re.match(opp_file_re, self.filename))

    @property
    def rfc3339(self):
        """Return RFC 3339 YYYY-MM-DDThh:mm:ss[+-]hh:mm parsed from filename"""
        if self.date:
            return self.date.isoformat(timespec='seconds')
        return ''

    @property
    def sort_key(self):
        # day of year from filename if possible first, then from path, then nothing
        if self.dayofyear:
            year, day = [int(x) for x in self.dayofyear.split("_")]
        elif self.path_dayofyear:
            year, day = [int(x) for x in self.path_dayofyear.split("_")]
        else:
            year, day = 0, 0
        if self.is_old_style:
            # Number part of basename, necessary because number isn't
            # zero-filled
            file_key = int(self.filename_noext.split(".")[0])
        else:
            file_key = self.filename_noext
        return (year, day, file_key)


def create_dayofyear_directory(dt):
    """Create SeaFlow day of year directory from a datetime object"""
    if dt:
        return "{}_{}".format(dt.year, dt.strftime('%j'))
    return ''


def timestamp_from_filename(filename):
    filename_noext = remove_ext(os.path.basename(filename))
    m = re.match(new_file_re, filename_noext)
    if m:
        # New style EVT filenames, e.g.
        # - 2014-05-15T17-07-08+00-00
        # - 2014-05-15T17-07-08-07-00
        # Parse RFC 3339 date string
        return "{date}T{hours}:{minutes}:{seconds}{tzhours}:{tzminutes}".format(**m.groupdict())
    raise ValueError('filename does not look like a new-style SeaFlow file')


def parse_path(file_path):
    """Return a dict with entries for 'dayofyear' dir and 'file' name"""
    d = {"dayofyear": '', "file": ''}
    parts = util.splitpath(file_path)
    d["file"] = parts[-1]
    if len(parts) > 1:
        if re.match(dayofyear_re, parts[-2]):
            d["dayofyear"] = parts[-2]
    return d


def remove_ext(filename):
    """Remove extensions from filename except .evt in old files."""
    file_parts = filename.split(".")
    noext = file_parts[0]
    if len(file_parts) > 1 and file_parts[1] == "evt" and re.match(r'^\d+$', file_parts[0]):
        # For old-style evt filenames, e.g. 42.evt
        noext += ".evt"
    return noext


def sorted_files(files):
    """Sort EVT/OPP/VCT file paths in chronological order.

    Order is based on day of year directory parsed from path and then file name.
    """
    sfiles = [SeaFlowFile(f) for f in files]
    return [s.path for s in sorted(sfiles, key=lambda x: x.sort_key)]


def filtered_file_list(total_list, filter_list):
    """
    Only keep files from total_list that are in filter_list.

    Match by file_id, but return original path in total_list.
    """
    filter_set = {SeaFlowFile(f).file_id for f in filter_list}
    files = []
    for f in total_list:
        if SeaFlowFile(f).file_id in filter_set:
            files.append(f)
    files = sorted_files(files)
    return files


def find_evt_files(root_dir, opp=False):
    """Return a chronologically sorted list of EVT/OPP file paths in root_dir."""
    files = util.find_files(root_dir)
    files = keep_evt_files(files, opp=opp)
    return sorted_files(files)


def keep_evt_files(files, opp=False):
    """Filter list of files to only keep EVT files."""
    files_list = []
    for f in files:
        try:
            sfile = SeaFlowFile(f)
        except errors.FileError:
            pass
        else:
            if (opp and sfile.is_opp) | (not opp and sfile.is_evt):
                files_list.append(f)
    return files_list


def timeselect_evt_files(sfiles, tstart, tend):
    """
    Filter a list of EVT files by datetime.datetime start and end.

    Parameters
    -----------
    sfiles: iterable of seaflowfile.SeaFlowFile
        Any files without dates will not be selected.
    tstart: str
        Start datetime. Pass None to remove lower bound.
    tend: str
        End datetime. Pass None to remove upper bound.

    Raises
    ------
    ValueError if tstart or tend can't be parsed.

    Returns
    -------
    List of seaflowfile.SeaFlowFile within tstart and tend.
    """
    sfiles = [f for f in sfiles if f.date is not None]
    if tstart is not None:
        sfiles = [f for f in sfiles if f.date >= tstart]
    if tend is not None:
        sfiles = [f for f in sfiles if f.date <= tend]
    return sfiles


def date_evt_files(evt_paths, sfl_df):
    """
    Create a DataFrame of file IDs, paths, timestamps.

    Only files in evt_paths and sfl_df["file"] that share file IDs will be
    included.

    Parameters
    -----------
    evt_paths: list of str
        EVT file paths.
    sfl_df: pandas.DataFrame
        DataFrame for SFL data, with "file" column for file IDs and "date" column
        with RFC3339 timestamp strings or datetime objects.

    Raises
    ------
    ValueError if dates can't be parsed.
    KeyError if "file" or "date" is missing from sfl_df.
    seaflowpy.errors.SeaFlowpyError if a filename can't be parsed.

    Returns
    -------
    pandas.DataFrame
        "file_id" column has file IDs, "path" has file paths, "date" has
        timestamp objects.
    """
    if not pd.api.types.is_datetime64_ns_dtype(sfl_df["date"]):
        sfl_df["date"] = sfl_df["date"].map(time.parse_date)
    sfl_dates_by_file = dict(zip(sfl_df["file"].tolist(), sfl_df["date"].tolist()))
    data = {"date": [], "file_id": [], "path": []}
    for path in evt_paths:
        file_id = SeaFlowFile(path).file_id
        if file_id in sfl_dates_by_file:
            data["file_id"].append(file_id)
            data["path"].append(path)
            data["date"].append(sfl_dates_by_file[file_id])
    return pd.DataFrame(data)[["date", "file_id", "path"]]
