class SeaFlowpyError(Exception):
    """Base class for all seaflowpy exceptions"""
    pass


class FileError(SeaFlowpyError):
    """Custom exception class for SeaFlow file format errors"""
    pass


class TooManyClustersError(SeaFlowpyError):
    """Custom exception class for when more than one bead cluster is found"""
    pass

class NoClusterError(SeaFlowpyError):
    """Custom exception class for when no bead cluster is found"""
    pass
