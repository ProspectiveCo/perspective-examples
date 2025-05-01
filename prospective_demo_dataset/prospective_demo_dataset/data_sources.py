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

import os
import io
import pandas as pd
from pydantic import BaseModel, Field, PrivateAttr, field_validator, ConfigDict
import datetime as dt
from enum import Enum
from typing import Optional, Any


# Fetch content from a URL using pyfetch (in Pyodide) or httpx (in standard Python).
try:
    # support for running inside Prospective notebooks within the Chrome browser
    from pyodide.http import pyfetch
    IN_PYODIDE = True
except ImportError:
    # support for running in standard Python environment
    IN_PYODIDE = False
    import httpx


async def fetch_text(url, **kwargs):
    """
    Fetch text content from a URL using pyfetch (in Pyodide) or httpx (in standard Python).
    """
    if IN_PYODIDE:
        response = await pyfetch(url, **kwargs)
        return await response.string()
    else:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, **kwargs)
            response.raise_for_status()
            return response.text

async def fetch_bytes(url, **kwargs):
    """
    Fetch binary content from a URL using pyfetch (in Pyodide) or httpx (in standard Python).
    """
    if IN_PYODIDE:
        response = await pyfetch(url, **kwargs)
        return await response.bytes()
    else:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, **kwargs)
            response.raise_for_status()
            return response.content



class SupportedFileTypes(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    ARROW = ".arrow"
    DATAFRAME = "dataframe"

_SUPPORTED_FILE_TYPES = [file_type.value for file_type in SupportedFileTypes]


_unnamed_source_counter = 0         # used to assign unique names to unnamed sources


async def fetch_url_to_dataframe(url: str, df_read_options: dict = {}) -> pd.DataFrame:
    """
    Fetch the data from the given URL and return it as a pandas DataFrame.
    """
    # Check if the URL is valid and starts with a supported protocol
    SUPPORTED_PROTOCOLS = ["http://", "https://", "s3://"]
    if not isinstance(url, str):
        raise ValueError("URL must be a string.")
    if not any(url.startswith(protocol) for protocol in SUPPORTED_PROTOCOLS):
        raise ValueError(f"URL must start with one of the following protocols: {', '.join(SUPPORTED_PROTOCOLS)}")
    # Get the file extension from the URL and check if it is supported
    _, ext = os.path.splitext(url)
    if ext not in _SUPPORTED_FILE_TYPES:
        raise ValueError(f"Invalid file type: {ext}. Supported file types are: {', '.join(_SUPPORTED_FILE_TYPES)}")
    # Convert S3 URL to HTTPS URL if necessary
    if url.startswith("s3://"):
        bucket_and_key = url[5:]  # Remove "s3://"
        bucket, _, key = bucket_and_key.partition("/")
        url = f"https://{bucket}.s3.amazonaws.com/{key}"
    # Read the data into a pandas DataFrame
    try:
        buffer = io.BytesIO(await fetch_bytes(url))
        if ext == ".csv":
            df = pd.read_csv(io.StringIO(buffer), **df_read_options)
        elif ext in {".parquet", ".arrow"}:
            if 'engine' not in df_read_options: df_read_options['engine'] = 'pyarrow'
            try: df = pd.read_parquet(io.StringIO(buffer), **df_read_options)
            except Exception as e:
                del df_read_options['engine']
                df = pd.read_feather(io.StringIO(buffer), df_read_options)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        raise ValueError(f"Failed to read data into DataFrame. Error: {e}")
    return df


class ProspectiveDemoDataSource(BaseModel):
    name: Optional[str] = Field(None, description="The name of the data source.")
    # -- TODO: change the source to be able to also take a http, s3 link and download the file first
    source: str | pd.DataFrame = Field(..., description="The source data to play. You can also pass a pandas DataFrame.")
    description: Optional[str] = Field(None, description="The description of the source data.")
    cols_description: Optional[dict[str, str]] = Field(None, description="The description of the columns of the source data.")
    # dataframe options
    df_read_options: Optional[dict] = Field({}, description="Additional options to pass to the pandas read function.")

    # private fields
    _df: pd.DataFrame = PrivateAttr(default=None)

    model_config = ConfigDict(
        extra="forbid",  # forbid extra fields
        arbitrary_types_allowed=True,  # allow arbitrary types
    )

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        if not isinstance(v, (str, pd.DataFrame)):
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        # validate the file extension in supported file types
        if isinstance(v, str):
            if not os.path.exists(v):
                raise ValueError(f"File {v} does not exist.")
            else:
                _, ext = os.path.splitext(v)
                if ext not in _SUPPORTED_FILE_TYPES:
                    raise ValueError(f"Invalid file type: {ext}. At this point only following file formats are supported: {', '.join(_SUPPORTED_FILE_TYPES)}")
        return v

    def model_post_init(self, __context):
        if isinstance(self.source, pd.DataFrame):
            self._df = self.source
        # --- validating the data source name:
        #  if name is not provided, set it to the filename
        if self.name is None:
            if isinstance(self.source, str):
                self.name = os.path.basename(self.source)
            else:
                # assign a unique name to the data source for dataframes and unnamed sources
                global _unnamed_source_counter
                _unnamed_source_counter += 1
                self.name = f"ds_{_unnamed_source_counter:03d}"
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
        # if we have previously read the dataframe, return it
        if self._df is not None:
            # logger.debug("PerspectiveDemoDataSource: Dataframe already read. Returning previously read dataframe.")
            return self._df
        # read the dataframe
        if isinstance(self.source, str):
            filetype = self.filetype
            if filetype == SupportedFileTypes.CSV:
                self._df = pd.read_csv(self.source, **self.df_read_options)
            elif filetype in {SupportedFileTypes.PARQUET, SupportedFileTypes.ARROW}:
                # set the engine to 'pyarrow' to avoid the warning message
                if 'engine' not in self.df_read_options: self.df_read_options['engine'] = 'pyarrow'
                try: self._df = pd.read_parquet(self.source, **self.df_read_options)
                except Exception as e:
                    del self.df_read_options['engine']
                    self._df = pd.read_feather(self.source, **self.df_read_options)
            else:
                raise ValueError(f"Invalid file type: {filetype}")
        elif isinstance(self.source, pd.DataFrame):
            self._df = self.source
        return self._df
    
    def get_df(self) -> pd.DataFrame:
        if self._df is None:
            self.read()
        return self._df
    
    def set_df(self, df: pd.DataFrame):
        """
        Set the dataframe. Useful for applying manual transformations to the dataframe.
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid dataframe.")
        self._df = df


class ProspectiveDemoStreamDataSource(ProspectiveDemoDataSource):
    frame_interval: str | float | dt.timedelta = Field(None, description="The time interval to advance the stream by the values in ts_col in each frame. ts_col must be provided. Either frame_nrows or frame_interval must be provided.")
    frame_nrows: int = Field(None, description="The number of rows to play in each frame. Either frame_nrows or frame_interval must be provided.")
    loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")

    # dataframe options
    ts_col: str = Field(None, description="The timestamp column of the source data.")

    # private fields
    _cur_index: int = PrivateAttr(0)

    @field_validator("frame_nrows")
    @classmethod
    def validate_frame_batch_size(cls, v):
        if v <= 0:
            raise ValueError("frame_nrows must be greater than 0.")
        return v
    
    @field_validator("frame_interval")
    @classmethod
    def validate_frame_interval(cls, v):
        try:
            pd.Timedelta(v)
        except Exception as e:
            raise ValueError(f"Invalid time interval: {v}.") from e
        return v

    def model_post_init(self, __context):
        # make sure either frame_batch_size or frame_interval is provided
        if not self.frame_nrows and not self.frame_interval:
            raise ValueError("Either frame_batch_size or frame_interval must be provided.")
        elif self.frame_nrows and self.frame_interval:
            raise ValueError("Only one of frame_batch_size or frame_interval must be provided.")
        # read the dataframe
        self.read()
        # validate the timestamp column and prepare for advancing frames by ts_interval
        if self.frame_interval is not None:
            self._validate_ts_col()
            self._prepare_for_ts_interval()

        # return the context
        return super().model_post_init(__context)
    
    def _validate_ts_col(self):
        if self.ts_col is None:
            raise ValueError("Timestamp column must be provided.")
        if self.ts_col not in self._df.columns:
            raise ValueError(f"Invalid timestamp column: {self.ts_col}.")
        # test to make sure ts_col is a datetime column, if not convert it
        if not pd.api.types.is_datetime64_any_dtype(self._df[self.ts_col]):
            self._df[self.ts_col] = pd.to_datetime(self._df[self.ts_col])

    def _prepare_for_ts_interval(self):
        # sort and index the dataframe by the timestamp column
        self._df.sort_values(by=self.ts_col, inplace=True)
        self._df.reset_index(drop=True, inplace=True)
    
    def next(self) -> pd.DataFrame:
        """
        Get the next frame of the stream.
        """
        if self._df is None:
            self.read()
        # ---- advance the stream by frame_nrows ----
        if self.frame_nrows:
            # loopback logic -- loop back to the beginning of the stream
            current_index = self._cur_index
            if current_index >= len(self._df) and not self.loopback: return None
            current_index = 0 if current_index >= len(self._df) else current_index
            # next frame logic
            next_frame = self._df.iloc[current_index:current_index+self.frame_nrows]    # get the next frame
            self._cur_index = current_index + self.frame_nrows                          # update the current index
            return next_frame
        # ---- advance the stream by frame_interval ----
        elif self.frame_interval:
            # loopback logic -- loop back to the beginning of the stream
            current_index = self._cur_index
            if current_index >= len(self._df) and not self.loopback: return None
            current_index = 0 if current_index >= len(self._df) else current_index
            # next frame logic
            current_ts = self._df[self.ts_col].iloc[current_index]                      # get the current timestamp
            next_ts = current_ts + pd.Timedelta(self.frame_interval)                 # calculate the next timestamp
            tmp = self._df[self._df[self.ts_col] >= next_ts]                            # find the next batch of rows
            next_index = tmp.index[0] if len(tmp) > 0 else len(self._df)                # get the next index
            next_frame = self._df.iloc[self._cur_index:next_index]                      # get the next frame
            self._cur_index = next_index                                                # update the current index
            # logger.debug(f"Current index: {current_index}, Next index: {next_index}, current_ts: {current_ts}, next_ts: {next_ts}, len: {len(next_frame)}")
            return next_frame
        else:
            raise ValueError("Invalid frame advancement method.")



class ProspectiveDemoBatchDataSource(ProspectiveDemoDataSource):
    """
    A data source that reads and returns the entire data in a single batch.

    This class is nearly identical to PerspectiveDemoDataSource, but it is provided for clarity and to avoid confusion.
    """

    def next(self) -> pd.DataFrame:
        """
        Get the next frame of the stream.
        """
        if self._df is None:
            self.read()
        return self._df



def test():
    data_filepath = r"data/generators_monthly_2023_md.parquet"
    # ds = PerspectiveDemoDataSource(source=data_filepath, loopback=False)
    # df = ds.read()
    # assert isinstance(df, pd.DataFrame)
    # print(df.head())

    # ---- testing frame_nrows ----
    # ds = PerspectiveDemoStreamDataSrouce(source=data_filepath, frame_nrows=10_000, loopback=True)
    # df = ds.read()
    # print(df.head())
    # print(f"Dataframe shape: {df.shape}, len={len(df)}")
    # counter = 0
    # while (frame := ds.next()) is not None:
    #     print('.', end='', flush=True)
    #     # print(f"Frame: len={len(frame)}")
    #     counter += 1
    #     if counter > 100:
    #         print("\nBreaking...")
    #         break
    # print("\nDone")
    
    # ---- testing frame_interval ----
    ts_col = 'report_date'
    ds = ProspectiveDemoStreamDataSource(source=data_filepath, frame_interval='1d', ts_col=ts_col, loopback=True)
    # let's look at the dataframe
    df = ds.read()
    print(f"Dataframe shape: {df.shape}, len={len(df)}")
    # print the min/max boundaries of the ts_col in the dataframe
    min_ts = df[ts_col].min()
    max_ts = df[ts_col].max()
    print(f"Timestamp column '{ts_col}' min: {min_ts}, max: {max_ts}")
    # show df records with report_date == 2023-01-02
    # print(df[df[ts_col] >= datetime(2023, 1, 2)].head())
    # play the stream
    counter = 0
    while (frame := ds.next()) is not None:
        print('.', end='', flush=True)
        # print(f"Frame: len={len(frame)}")
        counter += 1
        if counter > 100:
            print("\nBreaking...")
            break
    print("\nDone")


if __name__ == "__main__":
    test()
