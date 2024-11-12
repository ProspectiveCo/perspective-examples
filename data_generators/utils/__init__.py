
import logging
import sys

__all__ = [
    'logger'
]

def setup_logging(
        log_level = logging.INFO,
        logfile: str = None
    ) -> logging.Logger:
    """sets up python logging to console and an optional file.

    Args:
        log_level (logging.LEVEL, optional): Logging level. Defaults to logging.INFO.
        logfile (str, optional): Path to a logfile. Defaults to None which omits writing to a file.

    Returns:
        logging.Logger: python logger
    """
    # Create a custom logger
    logger = logging.getLogger('main')

    # overall setup
    log_level = logging.DEBUG
    log_formatter = logging.Formatter('[%(levelname)-8s][%(asctime)s](%(filename)s:%(lineno)d): %(message)s')
    # Set the global logging level to DEBUG
    logger.setLevel(logging.DEBUG)
    # setting up console logging
    console_handler = logging.StreamHandler(sys.stdout)  # Output to console
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    # setting file logging
    if logfile:
        file_handler = logging.FileHandler(logfile)  # Output to a file
        file_handler.setLevel(log_level)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)
    # return main logger
    return logger


# setup logging
logger: logging.Logger = setup_logging()
