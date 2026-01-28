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
import xarray as xr


def prepare_test_file(file_path: str, output_dir: str, size: float = 0.01):
    p: Path = Path(file_path)
    # Open the NetCDF file
    ds = xr.open_dataset(p)

    # Check if 'time' dimension exists
    if "time" not in ds.dims:
        print("Error: No 'time' dimension found in the NetCDF file.")
        ds.close()
        return

    # Get the total number of time steps
    total_time_steps = ds.dims["time"]

    # Calculate the number of time steps for size
    slice_size = max(1, int(total_time_steps * size))

    # Slice the dataset along the time dimension
    ds_slice = ds.isel(time=slice(0, slice_size))

    filename = p.name

    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    output_path = output_path / filename
    if output_path.exists():
        print(f"{output_path} exists, skipping...")
        return str(output_path)

    # Save the sliced dataset
    ds_slice.to_netcdf(output_path)

    print(f"Extracted first {size} ({slice_size} out of {total_time_steps} time steps)")
    print(f"Original time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    print(f"New time range: {ds_slice.time.values[0]} to {ds_slice.time.values[-1]}")

    print(output_path)
    # Close the datasets
    ds.close()
    ds_slice.close()


def download_file(url: str, auth: Tuple[str, str], output_directory: str) -> str:
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
    if output_path.exists():
        print(f"{output_path} exists, skipping...")
        return str(output_path)

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
    return str(output_path)


def main():
    chess_met_url = "https://catalogue.ceh.ac.uk/datastore/eidchub/835a50df-e74f-4bfb-b593-804fd61d5eab/dtr/chess-met_dtr_gb_1km_daily_20191201-20191231.nc"
    gear_hourly_url = "https://catalogue.ceh.ac.uk/datastore/eidchub/fc9423d6-3d54-467f-bb2b-fc7357a3941f/CEH-GEAR-1hr-v2_201612.nc"
    gear_daily_url = "https://catalogue.ceh.ac.uk/datastore/eidchub/dbf13dd5-90cd-457a-a986-f2f9dd97e93c/GB/daily/CEH_GEAR_daily_GB_2019.nc"

    data_urls = [
        (chess_met_url, "chess-met"),
        (gear_hourly_url, "gear-hourly"),
        (gear_daily_url, "gear-daily"),
    ]

    username = os.getenv("username")
    password = os.getenv("password")

    for url, name in data_urls:
        file_path: str = download_file(url, (username, password), f"data/{name}")
        prepare_test_file(file_path, f"data-tiny/{name}")


if __name__ == "__main__":
    load_dotenv()
    main()
