from __future__ import absolute_import
import gzip
import json
import os
import pandas as pd
import pprint
import re
from . import seaflowfile
from . import util
from collections import OrderedDict


class VCT(seaflowfile.SeaFlowFile):
    """Class for VCT per-particle population annotations"""

    def __init__(self, path=None, fileobj=None, read_data=True, vct=None):
        seaflowfile.SeaFlowFile.__init__(self, path, fileobj)
        # DataFrame with list of population labels in "pop" column
        if vct is None:
            self.vct = []
            self.vct_count = 0
        else:
            self.vct = vct
            self.vct_count = len(vct)

        if read_data and vct is None:
            self.read_vct()

    def __str__(self):
        keys = ["path", "file_id", "vct_count"]
        return json.dumps(OrderedDict([(k, getattr(self, k)) for k in keys]), indent=2)

    def read_vct(self):
        """Read a VCT text file and return a Pandas DataFrame."""
        with self.open() as fh:
            self.vct = pd.read_csv(fh, header=None, names=["pop"])
            self.vct_count = len(self.vct)

    def write_vct(self, outdir):
        outfile = os.path.join(outdir, self.file_id + ".vct")
        util.mkdir_p(os.path.dirname(outfile))
        if os.path.exists(outfile):
            os.remove(outfile)
        if os.path.exists(outfile + ".gz"):
            os.remove(outfile + ".gz")
        with gzip.open(outfile + ".gz", "wb") as f:
            string_ = "\n".join(self.vct["pop"].tolist()) + "\n"
            f.write(string_.encode())


def is_vct(file_path):
    """Does the file specified by this path look like a valid VCT file?"""
    # VCT file name regexes
    vct_re = re.compile(
        r'^(?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}(?:\..+)?\.vct|\d+\.evt(?:\..+)?\.vct)'
        r'(?:\.gz)?$'
    )
    parts = seaflowfile.parse_path(file_path)
    return bool(parts["file"] and vct_re.match(parts["file"]))


def find_vct_files(root_dir):
    """Return a chronologically sorted list of VCT file paths in root_dir."""
    files = util.find_files(root_dir)
    files = parse_file_list(files)
    return seaflowfile.sorted_files(files)


def parse_file_list(files):
    """Filter list of files to only keep VCT files."""
    files_list = []
    for f in files:
        if is_vct(f):
            files_list.append(f)
    return files_list
