import logging
import pytest
import pandas as pd
import io
import os
import prospective_demo_dataset.data_sources as pdds


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_ds_from_url():
    url = "https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pudl/generators_monthly_2022-2023.parquet"
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
    ds = pdds.ProspectiveDemoStreamDataSource(source=url, frame_interval='1d', ts_col=ts_col, loopback=True)
    # let's look at the dataframe
    df = await ds.read()
    logger.info(f"Dataframe shape: {df.shape}, len={len(df)}")
    logger.debug(df.head())
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] > 0
    # print(f"Dataframe shape: {df.shape}, len={len(df)}")
    # # print the min/max boundaries of the ts_col in the dataframe
    # min_ts = df[ts_col].min()
    # max_ts = df[ts_col].max()
    # print(f"Timestamp column '{ts_col}' min: {min_ts}, max: {max_ts}")
    # # show df records with report_date == 2023-01-02
    # # print(df[df[ts_col] >= datetime(2023, 1, 2)].head())
    # # play the stream
    # counter = 0
    # while (frame := ds.next()) is not None:
    #     print('.', end='', flush=True)
    #     # print(f"Frame: len={len(frame)}")
    #     counter += 1
    #     if counter > 100:
    #         print("\nBreaking...")
    #         break
    # print("\nDone")