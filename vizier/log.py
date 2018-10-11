import logging

def get_logger(level=logging.DEBUG, name='root'):
    """Returns a "global" logger.

    Args:
        level (optional): The logging level.
        name (optional): The name of the logger.

    Examples:
        >>> get_logger()

        >>> get_logger(level=logging.DEBUG, name='root')

    """
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger
