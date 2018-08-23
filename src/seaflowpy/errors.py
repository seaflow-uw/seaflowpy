class SeaFlowpyError(Exception):
    """Base class for all seaflowpy exceptions"""
    pass


class SimpleCalledProcessError(SeaFlowpyError):
    """Custom exception to replace subprocess.CalledProcessError

    subprocess.CalledProcessError does not handling pickling/unpickling through
    a multiprocessing pool very well (https://bugs.python.org/issue9400).
    """
    pass


class FileError(SeaFlowpyError):
    """Custom exception class for SeaFlow file format errors"""
    pass
