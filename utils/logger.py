
import logging
import logging.handlers
import sys
import os
from .config_loader import config


# ==============================================================================
# setup logging
# ==============================================================================

def setup_logging_simple(
    log_level=logging.INFO,
    logfile: str = None,
    logger_name: str = 'main'
    ) -> logging.Logger:
    """sets up python logging to console and an optional file.

    Args:
    log_level (logging.LEVEL, optional): Logging level. Defaults to logging.INFO.
    logfile (str, optional): Path to a logfile. Defaults to None which omits writing to a file.
    logger_name (str, optional): Name of the logger. Defaults to 'main'.

    Returns:
    logging.Logger: python logger
    """
    # Create a custom logger
    logger = logging.getLogger(logger_name)

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


def setup_logging_from_config(logger_config: dict, logger_name: str = 'main') -> logging.Logger:
    """sets up python logging based on a configuration dictionary.

    Args:
    logger_config (dict): Logging configuration dictionary.
    logger_name (str, optional): Name of the logger. Defaults to 'main'.

    Returns:
    logging.Logger: python logger
    """
    # Create a custom logger
    logger = logging.getLogger(logger_name)

    # Set the logging level
    valid_levels = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']
    if logger_config.get('level', 'INFO').upper() not in valid_levels:
        logger_config['level'] = 'INFO'
    log_level = getattr(logging, logger_config.get('level', 'INFO').upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create formatter
    log_formatter = logging.Formatter(
        # fmt=logger_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        fmt=logger_config.get('format', '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'),
        datefmt=logger_config.get('datefmt', '%Y-%m-%d %H:%M:%S')
    )

    # Add handlers
    for handler in logger_config.get('handlers', ['console']):
        if handler == 'console':
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.setFormatter(log_formatter)
            logger.addHandler(console_handler)
        elif handler == 'file':
            file_config = logger_config.get('file', {})
            file_path = file_config.get('path', 'app.log')
            max_size = file_config.get('max_size', '10MB')
            backup_count = file_config.get('backup_count', 3)
            max_bytes = int(max_size[:-2]) * 1024 * 1024  # Convert MB to bytes
            file_handler = logging.handlers.RotatingFileHandler(file_path, maxBytes=max_bytes, backupCount=backup_count)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(log_formatter)
            logger.addHandler(file_handler)

    return logger

# Check if the config has logging defined and if there's a file handler
if 'logging' in config and 'handlers' in config['logging'] and 'file' in config['logging']['handlers']:
    file_config = config['logging'].get('file', {})
    file_path = file_config.get('path', 'app.log')
    file_dir = os.path.dirname(file_path)
    # Check if the parent directory exists, if not, create it
    if file_dir and not os.path.exists(file_dir):
        try:
            os.makedirs(file_dir, exist_ok=True)
        except Exception as e:
            pass

# ==============================================================================
# global app logger
logger: logging.Logger = setup_logging_from_config(config.get('logging', {}))
# print an app startup message
logger.info('-' * 80)
logger.info('App: started')
