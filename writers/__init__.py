from abc import ABC, abstractmethod
import pandas as pd
import os 


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


class CsvWriter(DataWriter):
    def __init__(self, 
                 file_path: str,
                 mode: str = "w",                               # mode: The mode to open the file in. Default is 'w' (write). Other options are 'a' (append).
                 sep: str = ",",
                 lineterminator: str = "\n",
                 date_format: str = "%Y-%m-%dT%H:%M:%S.%f",
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.mode = mode
        self.sep = sep
        self.lineterminator = lineterminator
        self.date_format = date_format
        # Remove the file if it already exists and the mode is 'w'
        if self.mode == 'w' and os.path.exists(self.file_path):
            os.remove(self.file_path)

    def write(self, data: pd.DataFrame) -> None:
        data.to_csv(self.file_path, index=False, mode='a', sep=self.sep, line_terminator=self.lineterminator, date_format=self.date_format)

    def close(self) -> None:
        pass

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "file_path": "str"
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return CsvWriter(**config)
    

class ConsoleWriter(DataWriter):
    def write(self, data: pd.DataFrame) -> None:
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(data)

    def close(self) -> None:
        pass

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {}

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return ConsoleWriter(**config)
    

class ParquetWriter(DataWriter):
    def __init__(self, 
                 file_path: str,
                 mode: str = "w",                               # mode: The mode to open the file in. Default is 'w' (write). Other options are 'a' (append).
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.mode = mode
        # Remove the file if it already exists and the mode is 'w'
        if self.mode == 'w' and os.path.exists(self.file_path):
            os.remove(self.file_path)

    def write(self, data: pd.DataFrame) -> None:
        data.to_parquet(self.file_path, index=False, mode='a')

    def close(self) -> None:
        pass

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "file_path": "str"
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return ParquetWriter(**config)
    

class ArrowWriter(DataWriter):
    def __init__(self, 
                 file_path: str,
                 mode: str = "w",                               # mode: The mode to open the file in. Default is 'w' (write). Other options are 'a' (append).
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.mode = mode
        # Remove the file if it already exists and the mode is 'w'
        if self.mode == 'w' and os.path.exists(self.file_path):
            os.remove(self.file_path)

    def write(self, data: pd.DataFrame) -> None:
        data.to_feather(self.file_path)

    def close(self) -> None:
        pass

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "file_path": "str"
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return ArrowWriter(**config)


# Writer Registry
writer_registry = {
    "csv": CsvWriter,
    "console": ConsoleWriter,
    "parquet": ParquetWriter,
    "arrow": ArrowWriter,
}