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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field, PrivateAttr, field_validator, ConfigDict
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Any


class SupportedFileTypes(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"
    ARROW = ".arrow"
    DATAFRAME = "dataframe"

_SUPPORTED_FILE_TYPES = [file_type.value for file_type in SupportedFileTypes]


class PerspectiveDemoDataSource(BaseModel):
    source: str | Any = Field(..., description="The source data to play. You can also pass a pandas DataFrame.")
    description: Optional[str] = Field(None, description="The description of the source data.")
    cols_description: Optional[dict[str, str]] = Field(None, description="The description of the columns of the source data.")

    # dataframe options
    # loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")
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
            print("Returning previously read dataframe.")
            return self._df
        # read the dataframe
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


class PerspectiveDemoStreamDataSrouce(PerspectiveDemoDataSource):
    # speed: float = Field(1.0, description="The speed at which to play the stream.")
    frame_ts_interval: str | float | timedelta = Field(None, description="The time interval to advance the stream by the values in ts_col in each frame. ts_col must be provided. Either frame_nrows or frame_ts_interval must be provided.")
    frame_nrows: int = Field(None, description="The number of rows to play in each frame. Either frame_nrows or frame_ts_interval must be provided.")
    loopback: bool = Field(True, description="Whether to loop back to the beginning of the stream when the end is reached.")

    # dataframe options
    # index_cols: str | list[str] = Field(None, description="The index column of the source data.")
    ts_col: str = Field(None, description="The timestamp column of the source data.")
    # indexing_method: str = Field("ts_idx", description="The indexing method to use. Options are 'ts_idx', 'idx', or 'ts'.")

    # private fields
    _cur_index: int = PrivateAttr(0)

    @field_validator("frame_nrows")
    @classmethod
    def validate_frame_batch_size(cls, v):
        if v <= 0:
            raise ValueError("frame_nrows must be greater than 0.")
        return v
    
    @field_validator("frame_ts_interval")
    @classmethod
    def validate_frame_ts_interval(cls, v):
        try:
            pd.Timedelta(v)
        except Exception as e:
            raise ValueError(f"Invalid time interval: {v}.") from e
        return v

    def model_post_init(self, __context):
        # make sure either frame_batch_size or frame_ts_interval is provided
        if not self.frame_nrows and not self.frame_ts_interval:
            raise ValueError("Either frame_batch_size or frame_ts_interval must be provided.")
        elif self.frame_nrows and self.frame_ts_interval:
            raise ValueError("Only one of frame_batch_size or frame_ts_interval must be provided.")
        # read the dataframe
        self.read()
        # validate the timestamp column and prepare for advancing frames by ts_interval
        if self.frame_ts_interval is not None:
            self._validate_ts_col()
            self._prepare_for_ts_interval()

        # return the context
        return super().model_post_init(__context)
    
    def _validate_ts_col(self):
        print("Validating timestamp column...")
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
            # get the current index
            current_index = self._cur_index
            # loopback logic -- loop back to the beginning of the stream
            if current_index >= len(self._df):
                if self.loopback:
                    current_index = 0
                else:
                    return None
            # get the next frame
            next_frame = self._df.iloc[current_index:current_index+self.frame_nrows]
            # update the current index
            self._cur_index = current_index + self.frame_nrows
            return next_frame
        # ---- advance the stream by frame_ts_interval ----
        elif self.frame_ts_interval:
            # get the current index
            current_index = self._cur_index
            # loopback logic -- loop back to the beginning of the stream
            if current_index >= len(self._df):
                if self.loopback:
                    current_index = 0
                else:
                    return None
            current_ts = self._df[self.ts_col].iloc[current_index]                  # get the current timestamp
            next_ts = current_ts + pd.Timedelta(self.frame_ts_interval)             # calculate the next timestamp

            # ---- TODO: handle the case where the next_ts is greater than the max timestamp in the dataframe ----
            # ---- TODO: possibly implement keeping curr_ts and next_ts in a queue and advancing the stream by the difference between the two timestamps ----

            next_index = self._df[self._df[self.ts_col] >= next_ts].index[0]        # find the index of the next timestamp
            next_frame = self._df.iloc[self._cur_index:next_index]                  # get the next frame
            # update the current index
            self._cur_index = next_index
            print(f"Current index: {current_index}, Next index: {next_index}, current_ts: {current_ts}, next_ts: {next_ts}, len: {len(next_frame)}")
            return next_frame
        else:
            raise ValueError("Invalid frame advancement method.")



def test():
    data_filepath = r"/home/warthog/work/perspective/perspective-examples/data/generators_monthly_2023_md.parquet"
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
    
    # ---- testing frame_ts_interval ----
    ts_col = 'report_date'
    ds = PerspectiveDemoStreamDataSrouce(source=data_filepath, frame_ts_interval='1d', ts_col=ts_col, loopback=False)
    # let's look at the dataframe
    df = ds.read()
    print(df.head())
    print(f"Dataframe shape: {df.shape}, len={len(df)}")
    # print the min/max boundaries of the ts_col in the dataframe
    min_ts = df[ts_col].min()
    max_ts = df[ts_col].max()
    print(f"Timestamp column '{ts_col}' min: {min_ts}, max: {max_ts}")
    print(df[ts_col].head(n=100000))
    # # play the stream
    # counter = 0
    # while (frame := ds.next()) is not None:
    #     # print('.', end='', flush=True)
    #     # print(f"Frame: len={len(frame)}")
    #     counter += 1
    #     if counter > 100:
    #         print("\nBreaking...")
    #         break
    # print("\nDone")


if __name__ == "__main__":
    test()
