import pytest
import xarray as xr
from xarray.testing import assert_allclose
import tempfile
import shutil
from pathlib import Path
from dri_gridded_data.chessmet.converter import convert_chessmet
from dri_gridded_data.utils import load_yaml_config, Config


@pytest.mark.integration
@pytest.mark.slow
def test_convert_chessmet():
    with (
        tempfile.TemporaryDirectory() as temp_netcdf_dir,
        tempfile.TemporaryDirectory() as temp_zarr_dir,
    ):
        netcdf_source: Path = Path(
            "data/chess-met/chess-met_dtr_gb_1km_daily_20191201-20191231.nc"
        )

        shutil.copy2(netcdf_source, temp_netcdf_dir)

        config: Config = load_yaml_config("config/chessmet_default.yaml")
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
