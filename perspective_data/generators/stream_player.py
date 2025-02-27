import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, PrivateAttr, field_validator, ConfigDict
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional


class SupportedFileTypes(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    ARROW = ".arrow"
    DATAFRAME = "dataframe"

_SUPPORTED_FILE_TYPES = [file_type.value for file_type in SupportedFileTypes]


class StreamDataSource(BaseModel):
    source: str | pd.DataFrame = Field(..., description="The source data to play.")
    description: Optional[str] = Field(None, description="The description of the source data.")
    cols_description: Optional[dict[str, str]] = Field(None, description="The description of the columns of the source data.")

    # dataframe options
    loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")
    df_read_options: Optional[dict] = Field({}, description="Additional options to pass to the pandas read function.")

    # private fields
    _df: pd.DataFrame = PrivateAttr(None)

    # model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        if not isinstance(v, (str, pd.DataFrame)):
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        if isinstance(v, str):
            # check to see if the file exists
            if not os.path.exists(v):
                raise ValueError(f"File {v} does not exist.")
            else:
                # check for valid file extension
                _, ext = os.path.splitext(v)
                if ext not in _SUPPORTED_FILE_TYPES:
                    raise ValueError(f"Invalid file type: {ext}. At this point only following file formats are supported: {', '.join(_SUPPORTED_FILE_TYPES)}")
        elif isinstance(v, pd.DataFrame):
            raise NotImplementedError("Pandas DataFrame source is not yet supported.")
        return v

    def model_post_init(self, __context):
        if isinstance(self.source, pd.DataFrame):
            self._df = self.source
        # read the dataframe
        self.read()
        # return the context
        return super().model_post_init(__context)

    def _get_file_extension(self):
        if isinstance(self.source, str):
            _, ext = os.path.splitext(self.source)
            return ext
        else:
            return None

    @property
    def filetype(self) -> SupportedFileTypes:
        if self.source is None:
            raise ValueError("Source must be provided before accessing the file type.")
        if isinstance(self.source, pd.DataFrame):
            return SupportedFileTypes.DATAFRAME
        elif isinstance(self.source, str):
            ext = self._get_file_extension()
            if ext == ".csv":
                return SupportedFileTypes.CSV
            elif ext == ".parquet":
                return SupportedFileTypes.PARQUET
            elif ext == ".arrow":
                return SupportedFileTypes.ARROW
            else:
                raise ValueError(f"Invalid file type: {ext}")
        else:
            raise ValueError("Unsupported source type.")

    def read(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df

        if isinstance(self.source, str):
            filetype = self.filetype
            if filetype == SupportedFileTypes.CSV:
                self._df = pd.read_csv(self.source, **self.df_read_options)
            elif filetype == SupportedFileTypes.PARQUET:
                self._df = pd.read_parquet(self.source, **self.df_read_options)
            elif filetype == SupportedFileTypes.ARROW:
                self._df = pd.read_feather(self.source, **self.df_read_options)
            else:
                raise ValueError(f"Invalid file type: {filetype}")
        elif isinstance(self.source, pd.DataFrame):
            raise NotImplementedError("Pandas DataFrame source is not yet supported.")
        else:
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        # return the dataframe
        return self._df
    

def test():
    data_filepath = r"/home/warthog/work/perspective/perspective-examples/data/generators_monthly_2023_md.parquet"
    reader = StreamDataSource(source=data_filepath, loopback=False)
    reader.open()
    while (df := reader.read()) is not None:
        print(".")
        assert isinstance(df, pd.DataFrame)
    print("Done.")


if __name__ == "__main__":
    test()
