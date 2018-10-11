import logging
import threading

_lock = threading.Lock()
_logger = None
_name = 'root'
_level = logging.DEBUG


def get_logger():
    """Returns a "global" logger.

    Args:
        level (optional): The logging level.
        name (optional): The name of the logger.

    Examples:
        >>> get_logger()
        >>> get_logger(level=logging.DEBUG, name='root')

    """

    global _lock
    global _logger

    with _lock:
        if(_logger is None):
            formatter = logging.Formatter(fmt='%(asctime)s:%(levelname)s:%(module)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(fmt=formatter)
            _logger = logging.getLogger(_name)
            _logger.addHandler(handler)
            _logger.setLevel(_level)
            # Don't propogate up to root logger
            _logger.propagate = False

        return _logger
