#  ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
#  ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
#  ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
#  ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
#  ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
#  ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
#  ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
#  ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
#  ┃ This file is part of the Perspective library, distributed under the terms ┃
#  ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
#  ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""
A module to download Perspective demo datasets from AWS S3 via https links.
"""

import os
import aiohttp
import asyncio

# from .utils import logger

DATA_FILES = {
    'superstore': r'https://cdn.jsdelivr.net/npm/superstore-arrow/superstore.lz4.arrow',
    'pudl': [
        r"https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pudl/generators_monthly_2023_md.parquet",
        r"https://perspective-demo-dataset.s3.us-east-1.amazonaws.com/pudl/generators_monthly_2022-2023_lg.parquet",
    ]
}


_READ_CHUNK_SIZE = 1024 * 1024  # 1MB


async def download_file(url, dest, overwrite=False):
    """
    Download a file from a URL to a destination on disk asynchronously.
    """
    if os.path.exists(dest) and not overwrite:
        # logger.info(f"File {dest} already exists. Skipping download.")
        print(f"File {dest} already exists. Skipping download.")
        return
    # logger.info(f"Downloading {url} to {dest}")
    print(f"Downloading {url} to {dest}")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            with open(dest, 'wb') as f:
                while True:
                    chunk = await response.content.read(_READ_CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)


def download_files(urls, dests, overwrite=False):
    """
    Download multiple files asynchronously.
    """
    async def main():
        tasks = [download_file(url, dest, overwrite) for url, dest in zip(urls, dests)]
        await asyncio.gather(*tasks)
    
    asyncio.run(main())


def _get_filename_from_url(url):
    return os.path.basename(url)


def download_dataset(dataset_name, root_dir: str = "../data", overwrite: bool = False):
    """
    Download a dataset from the Perspective demo datasets.
    """
    if dataset_name != 'all' and dataset_name not in DATA_FILES:
        raise ValueError(f"Dataset {dataset_name} not found.")
    if dataset_name == 'all':
        # recursively download all datasets
        for dataset in DATA_FILES:
            download_dataset(dataset, root_dir)
        return
    # download the dataset
    urls = DATA_FILES[dataset_name]
    if isinstance(urls, str):
        urls = [urls]
    dests = [_get_filename_from_url(url) for url in urls]
    dests = [os.path.join(root_dir, dest) for dest in dests]
    download_files(urls, dests, overwrite)


def test():
    # test get_filename_from_url with a sample url
    url = 'https://perspective.finos.org/examples/data/superstore.zip'
    assert _get_filename_from_url(url) == 'superstore.zip'
    print('get_filename_from_url test passed!')
    # test download_dataset with a sample dataset
    download_dataset('pudl', overwrite=True)



if __name__ == '__main__':
    test()
    print('Download complete!')

