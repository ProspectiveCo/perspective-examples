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
from prospective_demo_dataset.fetch import fetch_bytes, is_url, is_url_valid


class SupportedFileTypes(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    ARROW = ".arrow"
    DATAFRAME = "dataframe"

SUPPORTED_FILE_TYPES = [file_type.value for file_type in SupportedFileTypes]


_unnamed_source_counter = 0         # used to assign unique names to unnamed sources


class ProspectiveDemoDataSource(BaseModel):
    name: Optional[str] = Field(None, description="The name of the data source.")
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
    def validate_source(cls, v):
        if not isinstance(v, (str, pd.DataFrame)):
            raise ValueError("Source must be a pandas DataFrame or a path to a data file.")
        # validate the file extension in supported file types
        if isinstance(v, str) and not is_url(v):
            if not os.path.exists(v):
                raise ValueError(f"File {v} does not exist.")
            _, ext = os.path.splitext(v)
            if ext not in SUPPORTED_FILE_TYPES:
                raise ValueError(f"Invalid file type: {ext}. At this point only following file formats are supported: {', '.join(SUPPORTED_FILE_TYPES)}")
        return v

    def model_post_init(self, context):
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
        return super().model_post_init(context)

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
            raise ValueError(f"Unsupported source type. {type(self.source)} - {self.source}")

    async def read(self) -> pd.DataFrame:
        # if we have previously read the dataframe, return it
        if self._df is not None:
            return self._df
        filetype = self.filetype
        # check to see if the source is a URL
        if is_url(self.source):
            source = io.BytesIO(await fetch_bytes(self.source))
        else:
            source = self.source
        # read the dataframe based on the file type
        if filetype == SupportedFileTypes.CSV:
            self._df = pd.read_csv(source, **self.df_read_options)
        elif filetype in {SupportedFileTypes.PARQUET, SupportedFileTypes.ARROW}:
            # set the engine to 'pyarrow' to avoid the warning message
            if 'engine' not in self.df_read_options: self.df_read_options['engine'] = 'pyarrow'
            try: self._df = pd.read_parquet(source, **self.df_read_options)
            except Exception as e:
                del self.df_read_options['engine']
                self._df = pd.read_feather(source, **self.df_read_options)
        elif filetype == SupportedFileTypes.DATAFRAME:
            self._df = pd.DataFrame(self.source)
        else:
            raise ValueError(f"Invalid file type: {filetype}")
        return self._df
    
    async def get_df(self) -> pd.DataFrame:
        return await self.read()
    
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
    _initialized: bool = PrivateAttr(False)
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

    def model_post_init(self, context):
        # make sure either frame_batch_size or frame_interval is provided
        if not self.frame_nrows and not self.frame_interval:
            raise ValueError("Either frame_batch_size or frame_interval must be provided.")
        elif self.frame_nrows and self.frame_interval:
            raise ValueError("Only one of frame_batch_size or frame_interval must be provided.")
        # return the context
        return super().model_post_init(context)

    async def __init_read(self):
        """
        Initialize the data source. This is called when the data source is created.
        """
        if self._initialized:
            return
        await self.read()
        # validate the timestamp column and prepare for advancing frames by ts_interval
        if self.frame_interval is not None:
            self._validate_ts_col()
            self._prepare_for_ts_interval()
        self._initialized = True

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
    
    async def next(self) -> pd.DataFrame:
        """
        Get the next frame of the stream.
        """
        if not self._initialized:
            await self.__init_read()
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
            next_ts = current_ts + pd.Timedelta(self.frame_interval)                    # calculate the next timestamp
            tmp = self._df[self._df[self.ts_col] >= next_ts]                            # find the next batch of rows
            next_index = tmp.index[0] if len(tmp) > 0 else len(self._df)                # get the next index
            next_frame = self._df.iloc[self._cur_index:next_index]                      # get the next frame
            self._cur_index = next_index                                                # update the current index
            # logger.debug(f"Current index: {current_index}, Next index: {next_index}, current_ts: {current_ts}, next_ts: {next_ts}, len: {len(next_frame)}")
            return next_frame
        else:
            raise ValueError("Invalid frame advancement method.")
