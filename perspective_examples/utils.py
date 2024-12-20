
import sys
import os
import yaml
import logging
import logging.handlers




__all__ = [
    'config',
    'load_config_yaml',
    'logger',
]


# ==============================================================================
# CONFIGURATION
# ==============================================================================

def load_config_yaml(config_file: str = None, default_config: dict = {}) -> dict:
    """loads a yaml configuration file and merges it with default configuration.

    Args:
    default_config (dict): Default configuration values.
    config_file (str, optional): Path to the configuration file. Defaults to None.

    Returns:
    dict: Merged configuration dictionary.
    """
    if config_file is None:
        for root, _, files in os.walk('.'):
            for file in files:
                if file in ['config.yaml', 'config.yml']:
                    config_file = os.path.join(root, file)
                    break
            if config_file:
                break
        # If config file is still not found, walk up the directory tree to root
        if not config_file:
            current_dir = os.path.abspath('.')
            max_levels_up = 5
            while (current_dir != os.path.abspath(os.sep)) and (max_levels_up > 0):
                for file in os.listdir(current_dir):
                    if file in ['config.yaml', 'config.yml']:
                        config_file = os.path.join(current_dir, file)
                        break
                if config_file:
                    break
                current_dir = os.path.dirname(current_dir)
                max_levels_up -= 1
    # check if config file exists
    if not config_file:
        raise FileNotFoundError('No configuration file found.')

    # load config file
    if config_file:
        print(f'Loading configuration from: {config_file}')
        with open(config_file, 'r') as file:
            file_config = yaml.safe_load(file)
        # overwrite default config with file config
        default_config.update(file_config)

    return default_config


# Default configuration
DEFAULT_CONFIG = {
    'alpha_vantage': {
        'api_key': '',  # set your API key here
        'base_url': 'https://www.alphavantage.co/query',
        'ticker_interval': 'TIME_SERIES_DAILY'
    },
    'stocks_generator': {
        'tech_symbols': [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'FB', 'NFLX', 'NVDA', 'TSLA', 'INTC', 'CSCO', 'ADBE', 'ORCL', 'IBM', 'CRM', 'PYPL', 'AMD', 'TXN', 'QCOM', 'AVGO', 'SHOP', 'SNAP', 'TWTR', 'SQ', 'DOCU', 'UBER', 'LYFT', 'ZM', 'SPOT'
        ],
        'chip_symbols': [
            'INTC', 'NVDA', 'AMD', 'TXN', 'QCOM', 'AVGO'
        ],
        'socials_tickers': [
            'FB', 'TWTR', 'SNAP', 'PINS'
        ],
        'e_commerce_symbols': [
            'AMZN', 'SHOP', 'EBAY', 'ETSY', 'WMT', 'TGT'
        ],
        'ride_share_symbols': [
            'UBER', 'LYFT'
        ],
        'brokers': [
            'Slick Sam', 'Trading Tina', 'Money Mike', 'Clever Cathy', 'Profit Pete', 'Risky Rachel', 'Big Bucks Bob', 'Smart Susan', 'Lucky Luke'
        ]
    }
}

# look for default configuration file: config.yaml in this directory
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
if not os.path.exists(DEFAULT_CONFIG_PATH):
    print(f'WARNING: Default configuration file missing! No file found at: {DEFAULT_CONFIG_PATH}.')
    print('Using default configuration values.')

# global app configuration
config = load_config_yaml(DEFAULT_CONFIG_PATH, default_config=DEFAULT_CONFIG)

CONFIGS = config


# ==============================================================================
# LOGGING
# ==============================================================================

def setup_default_logging(
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


def _create_log_dir_if_needed():
    """Creates the log directory if it does not exist.

    Args:
    logfile (str): Path to the log file.
    """
    global config
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
    

# create logging directory if needed
_create_log_dir_if_needed()
# setup logger
logger: logging.Logger = setup_logging_from_config(config.get('logging', {}))


# ==============================================================================
# OTHER
# ==============================================================================
# print an app startup message
logger.info('-' * 80)
logger.info('App: started')
