import os
import pandas as pd
from .base import DataWriter


class FileWriter(DataWriter):
    def __init__(self, 
                 file_path: str,
                 type: str = "infer",                          # type: The type of the file. Default is 'infer'. Other options are 'csv', 'parquet', 'arrow', 'ndjson'.
                 mode: str = "w",                               # mode: The mode to open the file in. Default is 'w' (write). Other options are 'a' (append).
                 date_format: str = "%Y-%m-%dT%H:%M:%S.%f",
                 sep: str = ",",
                 lineterminator: str = "\n",
                 encoding: str = "utf-8",
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.mode = mode
        self.date_format = date_format
        self.sep = sep
        self.lineterminator = lineterminator
        self.encoding = encoding
        # infer file type
        if type == "infer":
            if file_path.endswith(".csv"):
                type = "csv"
            elif file_path.endswith(".parquet"):
                type = "parquet"
            elif file_path.endswith(".arrow"):
                type = "arrow"
            elif file_path.endswith(".ndjson"):
                type = "ndjson"
            else:
                raise ValueError(f"Unknown file type for file: {self.file_path}")
        self.type = type
        # Remove the file if it already exists and the mode is 'w'
        if self.mode == 'w' and os.path.exists(self.file_path):
            os.remove(self.file_path)

    def write(self, data: pd.DataFrame) -> None:
        # write data to file. Pick the correct method based on the file type
        if self.type == "csv":
            header = not os.path.exists(self.file_path)
            data.to_csv(self.file_path, index=False, header=header, mode='a', sep=self.sep, lineterminator=self.lineterminator, date_format=self.date_format, encoding=self.encoding)
        elif self.type == "parquet":
            data.to_parquet(self.file_path, index=False, mode='a')
        elif self.type == "arrow":
            data.to_feather(self.file_path)
        elif self.type == "ndjson":
            data.to_json(self.file_path, orient='records', lines=True, date_format=self.date_format, mode='a')
        else:
            raise ValueError(f"Unknown file type: {self.type}")
        
    def close(self) -> None:
        pass

    @staticmethod
    def required_parameters() -> dict[str, str]:
        return {
            "file_path": "str",
        }

    @staticmethod
    def from_config(config: dict) -> 'DataWriter':
        return FileWriter(**config)
