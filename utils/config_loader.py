
import yaml
import os


__all__ = [
    'config',
    'load_config_yaml',
]


# ==============================================================================
# load configuration
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
