import gzip
import json
import os
import pprint
import re
import util
from collections import OrderedDict
from operator import itemgetter


class SeaflowFile(object):
    """Base class for EVT/OPP/VCT file classes"""

    def __init__(self, path=None, fileobj=None):
        # If fileobj is set, read data from this object. The path will be used
        # to set the file name in the database and detect compression.
        self.path = path  # file path, local or in S3
        self.fileobj = fileobj  # data in file object
        # Identifer to match across file types (EVT/OPP/VCT)
        self.file_id = self._get_julian_path()

    def __str__(self):
        keys = ["path", "file_id"]
        return json.dumps(OrderedDict([(k, getattr(self, k)) for k in keys]), indent=2)

    def _isgz(self):
        """Is file gzipped?"""
        return self.path and self.path.endswith(".gz")

    def open(self):
        """Return a file-like object for reading."""
        handle = None
        if self.fileobj:
            if self._isgz():
                handle = gzip.GzipFile(fileobj=self.fileobj)
            else:
                handle = self.fileobj
        else:
            if self._isgz():
                handle = gzip.GzipFile(self.path)
            else:
                handle = open(self.path)
        return handle

    def _get_julian_path(self, remove_ext=True):
        """Get the file path with julian directory.

        If there is no julian directory in path, just return file name. If
        remove_ext is True, remove parts of file name that would not be included
        in the original EVT file name, including ".gz".
        """
        parts = parse_path(self.path)

        if remove_ext:
            file_parts = parts["file"].split(".")
            parts["file"] = file_parts[0]
            if len(file_parts) > 1:
                if file_parts[1] == "evt":
                    parts["file"] += ".evt"

        jpath = parts["file"]
        if parts["julian"]:
            jpath = os.path.join(parts["julian"], jpath)

        return jpath

    def is_old_style(self):
        """Is this old style file? e.g. 2014_185/1.evt."""
        parts = parse_path(self.file_id)
        old_re = re.compile(r'^\d+\.evt$')
        if old_re.match(parts["file"]):
            return True
        else:
            return False


def parse_path(file_path):
    """Return a dict with entries for 'julian' dir and 'file' name"""
    julian_re = re.compile(r'^20\d{2}_\d{1,3}$')
    d = { "julian": None, "file": None }
    parts = util.splitpath(file_path)
    if len(parts) == 1:
        d["file"] = parts[0]
    elif len(parts) > 1:
        d["file"] = parts[-1]
        if julian_re.match(parts[-2]):
            d["julian"] = parts[-2]
    return d


def sorted_files(files):
    """Sort EVT/OPP/VCT file paths in chronological order.

    Order is based on parsed julian day directory and file name.
    """
    data = []
    for f in files:
        s = SeaflowFile(f)
        parts = parse_path(s.file_id)
        if "julian" in parts:
            year, day = [int(x) for x in parts["julian"].split("_")]
        else:
            year, day = 0, 0
        if s.is_old_style():
            file_key = int(parts["file"].split(".")[0])  # number part of basename
        else:
            file_key = parts["file"]

        data.append((year, day, file_key, f))

    return [x[-1] for x in sorted(data)]


def find_file_index(files, findme):
    """Return the index of a file in a list of files, based on file_id.

    Raise ValueError if not found, just like list.index.
    """
    if findme is None:
        raise ValueError("file to find not specified")
    # Convert to SeaflowFile objects to find by canonical file_id
    files = [SeaflowFile(f) for f in files]
    findme = SeaflowFile(findme)
    for i, f in enumerate(files):
        if f.file_id == findme.file_id:
            return i
    raise ValueError("%s not in list" % findme.path)


def files_between(files, start_file, end_file):
    """Return a sublist of files that contains files from start through end.

    files will be traversed and returned in chronological order.
    """
    if files:
        files = sorted_files(files)  # make sure sorted chronologically
        try:
            start_idx = find_file_index(files, start_file)
        except ValueError:
            start_idx = 0  # start at beginning
        try:
            end_idx = find_file_index(files, end_file) + 1
        except ValueError:
            end_idx = len(files)
        if end_idx <= start_idx:
            raise ValueError("end file must be later than start file")
        return files[start_idx:end_idx]
