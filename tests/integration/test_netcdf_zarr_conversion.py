import pytest
import xarray as xr
from xarray.testing import assert_allclose
import tempfile
import shutil
from pathlib import Path
from dri_gridded_data.chessmet.converters import convert_chessmet
from dri_gridded_data.gear.converters import conver_gear_hourly
from dri_gridded_data.utils import Config


@pytest.mark.integration
def test_convert_chessmet():
    with (
        tempfile.TemporaryDirectory() as temp_netcdf_dir,
        tempfile.TemporaryDirectory() as temp_zarr_dir,
    ):
        netcdf_source: Path = Path(
            "data/chess-met/chess-met_dtr_gb_1km_daily_20191201-20191231.nc"
        )

        shutil.copy2(netcdf_source, temp_netcdf_dir)

        config: Config = Config(
            start_year=2019,
            end_year=2019,
            start_month=12,
            end_month=12,
            input_dir=str(temp_netcdf_dir),
            filename="chess-met_{varname}_gb_1km_daily_{start_date}-{end_date}.nc",
            varnames=["dtr"],
            date_format="%Y%m%d",
            target_root=str(temp_zarr_dir),
            store_name="chessmet_fulloutput_yearly_100km_chunks.zarr",
            target_chunks={"time": 365, "y": 100, "x": 100},
            num_workers=1,
            prune=0,
        )
        config.input_dir = str(temp_netcdf_dir)
        config.target_root = str(temp_zarr_dir)

        convert_chessmet(config)

        temp_netcdf: Path = Path(temp_netcdf_dir, netcdf_source.name)
        temp_zarr: Path = Path(temp_zarr_dir, config.store_name)

        netcdf_dataset: xr.Dataset = xr.open_dataset(temp_netcdf)
        zarr_dataset: xr.Dataset = xr.open_zarr(temp_zarr, consolidated=False)
        for dimension in netcdf_dataset.sizes:
            assert netcdf_dataset.sizes[dimension] == zarr_dataset.sizes[dimension]

        vars_to_drop = set(zarr_dataset.coords) - set(netcdf_dataset.coords)
        zarr_dataset = zarr_dataset.drop_vars(vars_to_drop, errors="ignore")

        variable_name: str = "dtr"
        assert_allclose(netcdf_dataset[variable_name], zarr_dataset[variable_name])


@pytest.mark.integration
@pytest.mark.slow
def test_convert_gear_hourly():
    with (
        tempfile.TemporaryDirectory() as temp_netcdf_dir,
        tempfile.TemporaryDirectory() as temp_zarr_dir,
    ):
        netcdf_source: Path = Path("data/gear-hourly/CEH-GEAR-1hr-v2_201612.nc")

        shutil.copy2(netcdf_source, temp_netcdf_dir)

        config: Config = Config(
            start_year=2016,
            end_year=2016,
            start_month=12,
            end_month=12,
            input_dir=str(temp_netcdf_dir),
            prefix="CEH-GEAR-1hr-v2_",
            suffix=".nc",
            target_root=str(temp_zarr_dir),
            store_name="gearhrly_fulloutput_15day_100km_chunks.zarr",
            target_chunks={"time": 360, "y": 100, "x": 100, "bnds": 2},
            num_workers=1,
            prune=0,
        )

        conver_gear_hourly(config)

        temp_netcdf: Path = Path(temp_netcdf_dir, netcdf_source.name)
        temp_zarr: Path = Path(temp_zarr_dir, config.store_name)

        netcdf_dataset: xr.Dataset = xr.open_dataset(temp_netcdf)
        zarr_dataset: xr.Dataset = xr.open_zarr(temp_zarr, consolidated=False)
        for dimension in netcdf_dataset.sizes:
            assert netcdf_dataset.sizes[dimension] == zarr_dataset.sizes[dimension]

        vars_to_drop = set(zarr_dataset.coords) - set(netcdf_dataset.coords)
        zarr_dataset = zarr_dataset.drop_vars(vars_to_drop, errors="ignore")

        variable_name = "rainfall_amount"
        assert_allclose(netcdf_dataset[variable_name], zarr_dataset[variable_name])
