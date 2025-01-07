from abc import ABC, abstractmethod
import pandas as pd


class DataWriter(ABC):
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @staticmethod
    @abstractmethod
    def required_parameters() -> dict[str, str]:
        pass

    @staticmethod
    @abstractmethod
    def from_config(config: dict) -> 'DataWriter':
        pass
