import gzip
import os
import pprint
import re
import util


class SeaflowFile(object):
    """Base class for EVT/OPP/VCT file classes"""

    def __init__(self, path=None, fileobj=None):
        # If fileobj is set, read data from this object. The path will be used
        # to set the file name in the database and detect compression.
        self.path = path  # file path, local or in S3
        self.fileobj = fileobj  # data in file object

    def __str__(self):
        return pprint.pformat({ "path": self.path })

    def _isgz(self):
        """Is file gzipped?"""
        return self.path and self.path.endswith(".gz")

    def _open(self):
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

    def get_julian_path(self, remove_ext=True):
        """Get the file path with julian directory.

        If there is no julian directory in path, just return file name. If
        remove_ext is True, remove ".gz", ".vct", ".opp" extensions in that
        order, but always keep any ".evt" extension.
        """
        parts = parse_path(self.path)
        jpath = parts["file"]
        if parts["julian"]:
            jpath = os.path.join(parts["julian"], jpath)
        if remove_ext:
            for ext in [".gz", ".vct", ".opp"]:
                if jpath.endswith(ext):
                    jpath = jpath[:-len(ext)]
        return jpath


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
