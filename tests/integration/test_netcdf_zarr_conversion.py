import pytest
import xarray as xr
from xarray.testing import assert_allclose
import tempfile
import shutil
from pathlib import Path
from dri_gridded_data.chessmet.converters import convert_chessmet
from dri_gridded_data.gear.converters import convert_gear_hourly, convert_gear_daily
from dri_gridded_data.utils import Config
from typing import Dict, Any, Callable


def _assert_datasets_equivalent(netcdf_path: Path, zarr_path: Path, variable_name: str):
    """Helper function to compare NetCDF and Zarr datasets."""

    netcdf_dataset = xr.open_dataset(netcdf_path)
    zarr_dataset = xr.open_zarr(zarr_path, consolidated=False)

    for dimension in netcdf_dataset.sizes:
        assert netcdf_dataset.sizes[dimension] == zarr_dataset.sizes[dimension]

    vars_to_drop = set(zarr_dataset.coords) - set(netcdf_dataset.coords)
    zarr_dataset = zarr_dataset.drop_vars(vars_to_drop, errors="ignore")

    assert_allclose(netcdf_dataset[variable_name], zarr_dataset[variable_name])


def _run_conversion_test(
    netcdf_source: str,
    config_params: Dict[str, Any],
    converter_func: Callable,
    variable_name: str,
):
    """Generic test runner for NetCDF to Zarr conversion."""
    with (
        tempfile.TemporaryDirectory() as temp_netcdf_dir,
        tempfile.TemporaryDirectory() as temp_zarr_dir,
    ):
        netcdf_source_path = Path(netcdf_source)
        shutil.copy2(netcdf_source_path, temp_netcdf_dir)

        config_params.update(
            {"input_dir": str(temp_netcdf_dir), "target_root": str(temp_zarr_dir)}
        )

        config = Config(**config_params)
        converter_func(config)

        temp_netcdf = Path(temp_netcdf_dir, netcdf_source_path.name)
        temp_zarr = Path(temp_zarr_dir, config.store_name)

        _assert_datasets_equivalent(temp_netcdf, temp_zarr, variable_name)


@pytest.mark.integration
def test_convert_chessmet():
    config_params = {
        "start_year": 2019,
        "end_year": 2019,
        "start_month": 12,
        "end_month": 12,
        "filename": "chess-met_{varname}_gb_1km_daily_{start_date}-{end_date}.nc",
        "varnames": ["dtr"],
        "date_format": "%Y%m%d",
        "store_name": "chessmet_fulloutput_yearly_100km_chunks.zarr",
        "target_chunks": {"time": 365, "y": 100, "x": 100},
        "num_workers": 1,
        "prune": 0,
    }

    _run_conversion_test(
        "data-tiny/chess-met/chess-met_dtr_gb_1km_daily_20191201-20191231.nc",
        config_params,
        convert_chessmet,
        "dtr",
    )


@pytest.mark.integration
def test_convert_gear_hourly():
    config_params = {
        "start_year": 2016,
        "end_year": 2016,
        "start_month": 12,
        "end_month": 12,
        "prefix": "CEH-GEAR-1hr-v2_",
        "suffix": ".nc",
        "store_name": "gearhrly_fulloutput_15day_100km_chunks.zarr",
        "target_chunks": {"time": 360, "y": 100, "x": 100, "bnds": 2},
        "num_workers": 1,
        "prune": 0,
    }

    _run_conversion_test(
        "data-tiny/gear-hourly/CEH-GEAR-1hr-v2_201612.nc",
        config_params,
        convert_gear_hourly,
        "rainfall_amount",
    )


@pytest.mark.integration
def test_convert_gear_daily():
    config_params = {
        "start_year": 2019,
        "end_year": 2019,
        "start_month": 1,
        "end_month": 1,
        "prefix": "CEH_GEAR_daily_GB_",
        "suffix": ".nc",
        "store_name": "geardaily_fulloutput_yearly_100km_chunks.zarr",
        "target_chunks": {"time": 360, "y": 100, "x": 100},
        "num_workers": 1,
        "prune": 0,
    }

    _run_conversion_test(
        "data-tiny/gear-daily/CEH_GEAR_daily_GB_2019.nc",
        config_params,
        convert_gear_daily,
        "rainfall_amount",
    )
