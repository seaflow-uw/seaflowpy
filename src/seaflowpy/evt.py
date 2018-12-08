import re
import sys
from . import util
from . import seaflowfile


def is_evt(file_path):
    """Does the file specified by this path look like a valid EVT file?"""
    # EVT/OPP file name regexes
    evt_re = re.compile(
        r'^(?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}[+-]\d{2}-?\d{2}(?:\.evt)?(?:\.opp)?|\d+\.evt)(?:\.opp)?'
        r'(?:\.gz)?$'
    )
    parts = seaflowfile.parse_path(file_path)
    return bool(parts["file"] and evt_re.match(parts["file"]))


def find_evt_files(root_dir):
    """Return a chronologically sorted list of EVT or OPP file paths in root_dir."""
    files = util.find_files(root_dir)
    files = parse_file_list(files)
    return seaflowfile.sorted_files(files)


def parse_file_list(files):
    """Filter list of files to only keep EVT files.

    If the first item in files is "-", assume input consists of lines of file
    paths read from stdin. Any further items in files will be ignored.
    """
    files_list = []
    if len(files) and files[0] == "-":
        for line in sys.stdin:
            f = line.rstrip()
            if is_evt(f):
                files_list.append(f)
    else:
        for f in files:
            if is_evt(f):
                files_list.append(f)
    return files_list
