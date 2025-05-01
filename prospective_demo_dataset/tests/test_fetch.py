import pytest
import pandas as pd
import io
import os
from prospective_demo_dataset.fetch import fetch_text, fetch_bytes

# URLs to test
urls = [
    # "s3://perspective-demo-dataset/pudl/generators_monthly_2022-2023.parquet",
    "https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pudl/generators_monthly_2022-2023.parquet",
    "https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/tmp/us_zips.csv",
]

@pytest.mark.asyncio
@pytest.mark.parametrize("url", urls)
async def test_fetch_text(url):
    """
    Test fetch_text function by fetching data from the given URL
    and ensuring it can be parsed using pandas.
    """
    text_data = await fetch_text(url)
    _, ext = os.path.splitext(url)
    # Determine file type and parse using pandas
    if ext == ".csv":
        df = pd.read_csv(io.StringIO(text_data))
    elif ext in [".parquet", ".arrow"]:
        pytest.skip("fetch_text is not suitable for binary formats like Parquet.")
    else:
        pytest.skip(f"Unsupported file type for URL: {url}")
    # Check if the DataFrame is not empty
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("url", urls)
async def test_fetch_bytes(url):
    """
    Test fetch_bytes function by fetching data from the given URL
    and ensuring it can be parsed using pandas.
    """
    binary_data = await fetch_bytes(url)
    _, ext = os.path.splitext(url)
    # Determine file type and parse using pandas
    if ext == ".csv":
        df = pd.read_csv(io.BytesIO(binary_data))
    elif ext in [".parquet", ".arrow"]:
        try: df = pd.read_parquet(io.BytesIO(binary_data))
        except: df = pd.read_feather(io.BytesIO(binary_data))
    else:
        pytest.skip(f"Unsupported file type for URL: {url}")
    # Check if the DataFrame is not empty
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape[0] > 0
