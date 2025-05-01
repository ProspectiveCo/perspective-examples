#  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
#  ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
#  ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
#  ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
#  ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
#  ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
#  ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
#  ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
#  ┃ This file is part of the Perspective library, distributed under the terms ┃
#  ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
#  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

import sys
import logging
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ConfigDict
import os
import logging.handlers

__all__ = [
    'settings',
    'logger',
]


# ==============================================================================
# CONFIGURATION
# ==============================================================================


class Settings(BaseSettings):
    log_level: str = Field('INFO', description='Logging level for the application. Allowed values are: DEBUG, INFO, WARNING, ERROR, CRITICAL')

    # model_config = ConfigDict(extra='allow',)

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), 'configs.env')
        extra = 'allow'

    @field_validator('log_level')
    def validate_log_level(cls, v):
        if v not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            raise ValueError('log_level must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL')
        return v

    def get_log_level(self):
        return getattr(logging, self.log_level.upper(), logging.INFO)
    

# Load settings from the environment file
settings = Settings()


default_configs = {
    'data_sources': {
        'pudl': {
            'module': 'perspective_demo.data_sources.pudl',
            'class': 'PUDLDataSource',
            'kwargs': {
                'start_date': '2018-01-01',
                'end_date': '2018-12-31',
                'freq': 'D',
            }
        },
        'stocks': {
            'module': 'perspective_demo.data_sources.stocks',
            'class': 'StocksDataSource',
            'kwargs': {
                'securities': [
                    "AAPL.N",
                    "AMZN.N",
                    "QQQ.N",
                    "NVDA.N",
                    "TSLA.N",
                    "FB.N",
                    "MSFT.N",
                    "TLT.N",
                    "XIV.N",
                    "YY.N",
                    "CSCO.N",
                    "GOOGL.N",
                    "PCLN.N",
                    "NFLX.N",
                    "BABA.N",
                    "INTC.N",
                    "V.N",
                    "JPM.N",
                    "WMT.N",
                    "DIS.N",
                    "PYPL.N",
                    "ADBE.N",
                    "CMCSA.N",
                    "PEP.N",
                    "KO.N",
                    "NKE.N",
                    "MRK.N",
                    "PFE.N",
                    "T.N",
                    "VZ.N",
                    "ORCL.N",
                    "IBM.N",
                ]
            }
        }
    }
}

# print(settings.sources_pudl_module)
print(settings.sources_pudl_module)

# ==============================================================================
# LOGGING
# ==============================================================================

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger('main')
    logger.setLevel(settings.get_log_level())
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('[%(levelname)-8s][%(asctime)s]: %(message)s'))
    logger.addHandler(handler)
    return logger


# setup logger
logger: logging.Logger = _setup_logging()


# ==============================================================================
# OTHER
# ==============================================================================
ascii_art = """
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ https://prospective.co                                                    ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""
print(ascii_art)
# print an app startup message
logger.info('-' * 80)
logger.info('App: Perspective Demo Started')
