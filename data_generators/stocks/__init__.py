import os
from data_generators.utils import load_config_yaml
from data_generators.utils import logger

# Define imports when using the * syntax
__all__ = [
    'stocks_generator_config',
    ]

# Define the default configuration for the stocks generator
__DEFAULT_STOCKS_GENERATOR_CONFIG__ = {
    'alpha_vantage': {
        'api_key': os.environ.get('ALPHA_VANTAGE_API_KEY', 'demo'),
        'base_url': 'https://www.alphavantage.co/query',
        'ticker_interval': 'TIME_SERIES_DAILY'
    },
}

# Load the configuration when the module is imported
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
stocks_generator_config = load_config_yaml(config_path, __DEFAULT_STOCKS_GENERATOR_CONFIG__)
logger.info('Loaded stocks generator configuration from %s', config_path)

# post configuration load processing
if not stocks_generator_config['alpha_vantage']['api_key']:
    stocks_generator_config['alpha_vantage']['api_key'] = os.environ.get('ALPHA_VANTAGE_API_KEY', 'demo')
if stocks_generator_config['alpha_vantage']['api_key'] == 'demo':
    logger.warning('Using the demo Alpha Vantage API in demo mode. Please set your own API key by assigning $ALPHA_VANTAGE_API_KEY for full functionality.')
else:
    masked_api_key = '*' * (len(stocks_generator_config['alpha_vantage']['api_key']) - 4) + stocks_generator_config['alpha_vantage']['api_key'][-4:]
    logger.info('Using Alpha Vantage API key: %s', masked_api_key)
