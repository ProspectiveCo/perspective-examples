import pytest
import pandas as pd
from prospective_demo_dataset.data_sources import fetch_text, fetch_bytes

# URLs to test
urls = [
    "s3://perspective-demo-dataset/pudl/generators_monthly_2022-2023.parquet",
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

    # Determine file type and parse using pandas
    if url.endswith(".csv"):
        df = pd.read_csv(pd.compat.StringIO(text_data))
    elif url.endswith(".parquet"):
        pytest.skip("fetch_text is not suitable for binary formats like Parquet.")
    else:
        pytest.fail(f"Unsupported file type for URL: {url}")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.asyncio
@pytest.mark.parametrize("url", urls)
async def test_fetch_bytes(url):
    """
    Test fetch_bytes function by fetching data from the given URL
    and ensuring it can be parsed using pandas.
    """
    binary_data = await fetch_bytes(url)

    # Determine file type and parse using pandas
    if url.endswith(".csv"):
        df = pd.read_csv(pd.compat.BytesIO(binary_data))
    elif url.endswith(".parquet"):
        df = pd.read_parquet(pd.compat.BytesIO(binary_data))
    else:
        pytest.fail(f"Unsupported file type for URL: {url}")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
