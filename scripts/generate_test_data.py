import xarray as xr
from pathlib import Path
import argparse


def main(netcdf_file: Path, output_dir: Path = Path("tests/data")) -> None:
    netcdf_dataset: xr.Dataset = xr.open_dataset(netcdf_file)

    x_middle = netcdf_dataset.sizes["x"] // 2
    y_middle = netcdf_dataset.sizes["y"] // 2
    sliced = netcdf_dataset.isel(
        x=slice(x_middle - 1, x_middle + 1), y=slice(y_middle - 1, y_middle + 1)
    )

    output_file: Path = output_dir / netcdf_file.name
    output_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing test data to {output_file}")
    sliced.to_netcdf(output_file)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate test data by extracting a subset from NetCDF files"
    )
    parser.add_argument("input_file", type=Path, help="Path to the input NetCDF file")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("tests/data"),
        help="Output directory for test data (default: tests/data)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.input_file, args.output_dir)
