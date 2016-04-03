class SeaflowpyError(Exception):
    """Base class for all seaflowpy exceptions"""
    pass


class SimpleCalledProcessError(SeaflowpyError):
    """Custom exception to replace subprocess.CalledProcessError

    subprocess.CalledProcessError does not handling pickling/unpickling through
    a multiprocessing pool very well (https://bugs.python.org/issue9400).
    """
    pass


class EVTFileError(SeaflowpyError):
    """Custom exception class for EVT file format errors"""
    pass
