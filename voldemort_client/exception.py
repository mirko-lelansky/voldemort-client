class VoldemortException(Exception):
    """
    This is the root exception class of the client.
    """
    pass

class ConnectionException(VoldemortException):
    """
    This is the base exception class for the connection handling.
    """
    pass
