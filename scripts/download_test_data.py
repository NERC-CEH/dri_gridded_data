"""
Script for downloading test data files from CEH data catalogue.

This module provides functionality to download gridded meteorological data files
from the Centre for Ecology & Hydrology (CEH) data catalogue with authentication.

Example:
    Set up .env file with credentials to access EIDC:

    username=your_username
    passsword=your_password

    Then run the script:
        $ uv run scripts/download_test_data.py
"""

from dotenv import load_dotenv
import os
import requests
from tqdm import tqdm
from typing import Tuple
from pathlib import Path
from urllib.parse import urlparse


def download_file(url: str, auth: Tuple[str, str], output_directory: str):
    """
    Download a file from a URL with authentication and display progress.

    Args:
        url (str): The URL of the file to download
        auth (Tuple[str, str]): Username and password tuple for authentication
        output_directory (str): Directory path where the file will be saved
    """
    dir_path: Path = Path(output_directory)
    dir_path.mkdir(exist_ok=True, parents=True)

    filename = Path(urlparse(url).path).name

    output_path = dir_path / filename

    response = requests.get(url, auth=auth, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    with (
        open(output_path, "wb") as file,
        tqdm(
            desc=str(output_path),
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress,
    ):
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                progress.update(len(chunk))


def main():
    chess_met_url = "https://catalogue.ceh.ac.uk/datastore/eidchub/835a50df-e74f-4bfb-b593-804fd61d5eab/dtr/chess-met_dtr_gb_1km_daily_20191201-20191231.nc"
    gear_hourly_url = "https://catalogue.ceh.ac.uk/datastore/eidchub/fc9423d6-3d54-467f-bb2b-fc7357a3941f/CEH-GEAR-1hr-v2_201612.nc"

    data_urls = [(chess_met_url, "chessmet"), (gear_hourly_url, "gear-hourly")]

    username = os.getenv("username")
    password = os.getenv("password")

    for url, dir in data_urls:
        download_file(url, (username, password), dir)


if __name__ == "__main__":
    load_dotenv()
    main()
