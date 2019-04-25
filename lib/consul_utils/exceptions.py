

class ConsulException(Exception):
    """
    Base exception for consul utils, this exception will be catch and log to error.
    """
    pass


class FilterStop(Exception):
    """
    Stop filter execution, this exception will be catch and not be reported.
    """
    pass
