import os
import logging
import pytest
import pandas as pd
import datetime as dt

import prospective_demo_dataset.data_sources as pdds


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_ds_from_url():
    url = "https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pudl/generators_monthly_2022-2023.parquet"
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


@pytest.mark.asyncio
@pytest.mark.dependency()
async def test_ds_local_csv():
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test_ds_001.csv'))
    ds = pdds.ProspectiveDemoDataSource(source=filepath)
    df = await ds.read()
    # test the data source naming
    assert ds.name == 'test_ds_001.csv', f"Extraction of data source name from it's URL or filepath not working. Expected 'test_ds_001.csv', got {ds.name}"
    # test file type
    assert ds.filetype == pdds.SupportedFileTypes.CSV, f"Extraction of file type from it's URL or filepath not working. Expected 'csv', got {ds.file_type}"
    # test reading source dataframe
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape == (10, 4), f"Expected dataframe shape (10, 4), got {df.shape}"


@pytest.mark.asyncio
@pytest.mark.dependency(depends=["test_ds_local_csv"])
async def test_dstrm_local_csv():
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test_ds_001.csv'))
    ds = pdds.ProspectiveDemoStreamDataSource(source=filepath, ts_col='date_value', frame_interval='1d', loopback=False)
    frame_count = 0
    curr_date = dt.date(2025, 1, 1)
    while (frame := await ds.next()) is not None:
        # test to see if the frame is a dataframe and contains exaxtly a single row
        assert isinstance(frame, pd.DataFrame)
        assert not frame.empty
        assert frame.shape == (1, 4), f"Expected dataframe shape (1, 4), got {frame.shape}"
        # test to see if the frame contains the expected date
        assert frame.iloc[0, 1].date() == curr_date, f"Expected date {curr_date}, got {frame.iloc[0, 1].date()}"
        curr_date += dt.timedelta(days=1)
        frame_count += 1
        if frame_count > 10:
            break
    assert frame_count == 10, f"Expected 10 frames, got {frame_count}"


@pytest.mark.asyncio
@pytest.mark.dependency()
async def test_ds_local_parquet():
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test_ds_001.parquet'))
    ds = pdds.ProspectiveDemoDataSource(source=filepath)
    df = await ds.read()
    # test the data source naming
    assert ds.name == 'test_ds_001.parquet', f"Extraction of data source name from it's URL or filepath not working. Expected 'test_ds_001.parquet', got {ds.name}"
    # test file type
    assert ds.filetype == pdds.SupportedFileTypes.PARQUET, f"Extraction of file type from it's URL or filepath not working. Expected 'parquet', got {ds.file_type}"
    # test reading source dataframe
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape == (10, 4), f"Expected dataframe shape (10, 4), got {df.shape}"


@pytest.mark.asyncio
@pytest.mark.dependency(depends=["test_ds_local_parquet"])
async def test_dstrm_local_parquet():
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'test_ds_001.parquet'))
    ds = pdds.ProspectiveDemoStreamDataSource(source=filepath, ts_col='date_value', frame_interval='1d', loopback=False)
    frame_count = 0
    curr_date = dt.date(2025, 1, 1)
    while (frame := await ds.next()) is not None:
        # test to see if the frame is a dataframe and contains exaxtly a single row
        assert isinstance(frame, pd.DataFrame)
        assert not frame.empty
        assert frame.shape == (1, 4), f"Expected dataframe shape (1, 4), got {frame.shape}"
        # test to see if the frame contains the expected date
        assert frame.iloc[0, 1].date() == curr_date, f"Expected date {curr_date}, got {frame.iloc[0, 1].date()}"
        curr_date += dt.timedelta(days=1)
        frame_count += 1
        if frame_count > 10:
            break
    assert frame_count == 10, f"Expected 10 frames, got {frame_count}"

