"""
This module is the exception module of the class and holds all application
exception classes.
"""

class VoldemortError(Exception):
    """
    This is the root exception class of the client.
    """
    pass

class RestException(VoldemortError):
    """
    This is the base exception class for the connection handling.
    """
    pass
