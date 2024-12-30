

import requests
import zipfile
import os


def download():
    # URL of the file to be downloaded
    url = "https://www.kaggle.com/api/v1/datasets/download/ehallmar/daily-historical-stock-prices-1970-2018"

    # Destination path for the downloaded file
    data_dir = r"../../data"
    zip_file = os.path.join(data_dir, "historical_stocks.zip")
    data_file = "historical_stock_prices.csv"  # The specific data file to extract

    # Step 1: Download the ZIP file
    # Download the file with streaming enabled
    print(f"Downloading from kaggle: {url}\nThis will take few minutes...")
    response = requests.get(url, stream=True)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Write the file content to the destination file in chunks
        with open(zip_file, "wb") as file:
            print(f"Writing to file: {zip_file}")
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Ensure the chunk is not empty
                    file.write(chunk)
        print("Download completed successfully.")
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        raise RuntimeError()

    # Step 2: Extract only the target file from the ZIP archive
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            # Check if the target file exists in the archive
            if data_file in zip_ref.namelist():
                print(f"Extracting zip file: {data_file}")
                zip_ref.extract(data_file, data_dir)
                print(f"Extracted {data_file} to {data_dir}")
            else:
                print(f"Error: {data_file} not found in the archive.")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid ZIP archive.")

    # step 3: clean up
    print(f"Removing downloaded zip file: {zip_file}")
    os.remove(zip_file)

