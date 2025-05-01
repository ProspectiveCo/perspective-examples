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

SUPPORTED_PROTOCOLS = ["http://", "https://", "s3://"]

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
        

def is_url(url: str) -> bool:
    """
    Check if the given URL is valid.
    """
    if not isinstance(url, str):
        return False
    if any(url.startswith(protocol) for protocol in SUPPORTED_PROTOCOLS):
        return True
    return False


async def is_url_valid(url: str) -> bool:
    """
    Check if a URL is valid (does not return a 404 error) using httpx.
    """
    if IN_PYODIDE:
        response = await pyfetch(url, method="HEAD")
        return response.ok
    else:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.head(url, timeout=5)
                return response.status_code == 200
            except httpx.RequestError:
                return False


async def s3_to_https(url: str) -> str:
    """
    Convert an S3 URL to an HTTPS URL.
    """
    AWS_REGIONS = [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "af-south-1", "ap-east-1", "ap-south-1", "ap-south-2",
        "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
        "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
        "ap-southeast-4", "ap-southeast-5", "ap-southeast-7",
        "ca-central-1", "ca-west-1", "eu-central-1", "eu-central-2",
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-north-1",
        "eu-south-1", "eu-south-2", "il-central-1", "mx-central-1",
        "me-south-1", "me-central-1", "sa-east-1"
    ]
    if url.startswith("s3://"):
        bucket_and_key = url[5:]  # Remove "s3://"
        bucket, _, key = bucket_and_key.partition("/")
        for region in AWS_REGIONS:
            https_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
            if await is_url_valid(https_url):
                return https_url
        raise ValueError("No valid URL found for the given S3 bucket and key.")
    else:
        return url


# async def fetch_df(url: str, **df_options) -> pd.DataFrame:
#     """
#     Fetch the data from the given URL and return it as a pandas DataFrame.
#     """
#     # Check if the URL is valid and starts with a supported protocol
#     SUPPORTED_PROTOCOLS = ["http://", "https://", "s3://"]
#     SUPPORTED_FILE_TYPES = [".csv", ".parquet", ".arrow"]
#     if not isinstance(url, str):
#         raise ValueError("URL must be a string.")
#     if not any(url.startswith(protocol) for protocol in SUPPORTED_PROTOCOLS):
#         raise ValueError(f"URL must start with one of the following protocols: {', '.join(SUPPORTED_PROTOCOLS)}")
#     # Get the file extension from the URL and check if it is supported
#     _, ext = os.path.splitext(url)
#     if ext not in SUPPORTED_FILE_TYPES:
#         raise ValueError(f"Invalid file type: {ext}. Supported file types are: {', '.join(SUPPORTED_FILE_TYPES)}")
#     # Convert S3 URL to HTTPS URL if necessary
#     if url.startswith("s3://"):
#         bucket_and_key = url[5:]  # Remove "s3://"
#         bucket, _, key = bucket_and_key.partition("/")
#         url = f"https://{bucket}.s3.amazonaws.com/{key}"
#     # Read the data into a pandas DataFrame
#     try:
#         buffer = io.BytesIO(await fetch_bytes(url))
#         if ext == ".csv":
#             df = pd.read_csv(buffer, **df_options)
#         elif ext in {".parquet", ".arrow"}:
#             if 'engine' not in df_options: df_options['engine'] = 'pyarrow'
#             try: df = pd.read_parquet(buffer, **df_options)
#             except Exception as e:
#                 del df_options['engine']
#                 df = pd.read_feather(buffer, df_options)
#         else:
#             raise ValueError(f"Unsupported file type: {ext}")
#     except Exception as e:
#         raise ValueError(f"Failed to read data into DataFrame. Error: {e}")
#     return df