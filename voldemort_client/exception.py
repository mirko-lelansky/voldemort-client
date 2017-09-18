class VoldemortException(Exception):
    """
    This is the root exception class of the client.
    """
    def __init__(self, msg, code = 1):
        self._code = code
        self._msg = msg

    def __str__(self):
        return self._msg
