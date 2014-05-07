

class Timeout(Exception):
    """ Raise when a timeout has been reached """

class AlreadyFired(Exception):
    """ Raised when an async result got a second result to handle """
