class SeaFlowpyError(Exception):
    """Base class for all seaflowpy exceptions"""
    pass


class FileError(SeaFlowpyError):
    """Custom exception class for SeaFlow file format errors"""
    pass


class ClusterError(SeaFlowpyError):
    """Custom exception class bead clustering errors"""
    pass
