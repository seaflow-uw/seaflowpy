from __future__ import print_function
from __future__ import absolute_import
from . import errors
from functools import wraps
from operator import itemgetter
from signal import getsignal, signal, SIGPIPE, SIG_DFL
import datetime
import errno
import os
import subprocess
import time


def find_files(root_dir):
    """Return a list of all file paths below root_dir."""
    allfiles = []
    for root, dirs, files in os.walk(root_dir):
        for f in files:
            allfiles.append(os.path.join(root, f))
    return allfiles


def gzip_file(path, print_timing=False):
    """Gzip a file.

    Try to use pigz, but fall back to gzip.
    """
    gzipbin = "pigz"  # Default to using pigz
    devnull = open(os.devnull, "w")
    try:
        subprocess.check_call(["pigz", "--version"], stdout=devnull,
                              stderr=subprocess.STDOUT)
    except OSError as e:
        # If pigz is not installed fall back to gzip
        gzipbin = "gzip"

    if print_timing:
        t0 = time.time()
        print("")
        print("Compressing %s" % path)

    try:
        output = subprocess.check_output([gzipbin, "-f", path],
                                         stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise errors.SimpleCalledProcessError(e.output)

    if print_timing:
        t1 = time.time()
        print("Compression completed in %.2f seconds" % (t1 - t0))


def mkdir_p(path):
    """Create directory tree for path."""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def splitpath(path):
    """Return a list of all path components"""
    parts = []
    path, last = os.path.split(path)
    if last != "":
        parts.append(last)
    while True:
        path, last = os.path.split(path)
        if last != "":
            parts.append(last)
        else:
            if path != "":
                parts.append(path)
            break
    return parts[::-1]


def suppress_sigpipe(f):
    """Decorator to handle SIGPIPE cleanly.

    Prevent Python from turning SIGPIPE into an exception and printing an
    uncatchable error message. Note, if the wrapped function depends on the
    default behavior of Python when handling SIGPIPE this decorator may have
    unintended effects."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        orig_handler = getsignal(SIGPIPE)
        signal(SIGPIPE, SIG_DFL)
        try:
            f(*args, **kwargs)
        finally:
            signal(SIGPIPE, orig_handler)  # restore original Python SIGPIPE handler
    return wrapper
