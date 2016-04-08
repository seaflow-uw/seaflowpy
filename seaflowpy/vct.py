import gzip
import json
import pandas as pd
import pprint
import re
import seaflowfile
import util
from collections import OrderedDict


class VCT(seaflowfile.SeaflowFile):
    """Class for VCT per-particle population annotations"""

    def __init__(self, path=None, fileobj=None, read_data=True):
        seaflowfile.SeaflowFile.__init__(self, path, fileobj)
        self.vct_count = 0
        self.vct = None  # DataFrame with list of population labels

        if read_data:
            self._read_vct()

    def __str__(self):
        keys = ["path", "file_id", "vct_count"]
        return json.dumps(OrderedDict([(k, getattr(self, k)) for k in keys]), indent=2)

    def _read_vct(self):
        """Read a VCT text file and return a Pandas DataFrame."""
        with self._open() as fh:
            self.vct = pd.read_csv(fh, header=None, names=["pop"])
            self.vct_count = len(self.vct)


def is_vct(file_path):
    """Does the file specified by this path look like a valid VCT file?"""
    # VCT file name regexes
    vct_re = re.compile(
        r'^(?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}\.vct|\d+\.vct)'
        r'(?:\.gz)?$'
    )
    parts = seaflowfile.parse_path(file_path)
    return bool(parts["file"] and vct_re.match(parts["file"]))


def find_vct_files(root_dir):
    """Return a sorted list of all VCT file paths in root_dir."""
    files = util.find_files(root_dir)
    files = parse_vct_file_list(files)
    return sorted(files)


def parse_vct_file_list(files):
    """Filter list of files to only keep VCT files."""
    files_list = []
    for f in files:
        if is_vct(f):
            files_list.append(f)
    return files_list
