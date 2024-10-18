# MJB (UKCEH) Aug-2024
# Example script for a pangeo-forge-recipe to convert
# gridded netcdf files to a zarr datastore ready for upload
# to object storage.
# See jupyter notebook for more details and explanations/comments
# Please note that this script/notebook is intended to serve as an example only,
# and be adapted for your own datasets.

import os
import sys
import apache_beam as beam
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from apache_beam.options.pipeline_options import PipelineOptions
from pangeo_forge_recipes.transforms import (
        OpenWithXarray,
        StoreToZarr,
        ConsolidateDimensionCoordinates,
        ConsolidateMetadata,
        T,    
        )
from pangeo_forge_recipes.types import Indexed

from GEAR_config import load_yaml_config

# Add in our own custom Beam PTransform (Parallel Transform) to apply
# some preprocessing to the dataset. In this case to convert the
# 'bounds' variables to coordinate rather than data variables.

# They are implemented as subclasses of the beam.PTransform class
class DataVarToCoordVar(beam.PTransform):

    # not sure why it needs to be a staticmethod
    @staticmethod
    # the preprocess function should take in and return an
    # object of type Indexed[T]. These are pangeo-forge-recipes
    # derived types, internal to the functioning of the
    # pangeo-forge-recipes transforms.
    # I think they consist of a list of 2-item tuples,
    # each containing some type of 'index' and a 'chunk' of
    # the dataset or a reference to it, as can be seen in
    # the first line of the function below
    def _datavar_to_coordvar(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        # do something to each ds chunk here 
        # and leave index untouched.
        # Here we convert some of the variables in the file
        # to coordinate variables so that pangeo-forge-recipes
        # can process them
        print(f'Preprocessing before {ds =}')
        ds = ds.set_coords(['x_bnds', 'y_bnds', 'time_bnds', 'crs'])
        print(f'Preprocessing after {ds =}')
        return index, ds

    # this expand function is a necessary part of
    # developing your own Beam PTransforms, I think
    # it wraps the above preprocess function and applies
    # it to the PCollection, i.e. all the 'ds' chunks in Indexed
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._datavar_to_coordvar)

def main(config_file_path):
    config = load_yaml_config(config_file_path)
    
    if not os.path.exists(config.target_root):
        os.makedirs(config.target_root)
    
    def make_path(time):
        filename = config.prefix + time + config.suffix
        print(f"FILENAME: {filename}")
        return os.path.join(config.input_dir, filename)
    
    years = list(range(config.start_year, config.end_year + 1))
    months = list(range(config.start_month, config.end_month))
    ymonths = [f"{year}{month:02d}" for year in years for month in months]
    time_concat_dim = ConcatDim("time", ymonths)
    
    pattern = FilePattern(make_path, time_concat_dim)
    if config.prune > 0:
        pattern = pattern.prune(nkeep=config.prune)

    recipe = (
        beam.Create(pattern.items())
        | OpenWithXarray(file_type=pattern.file_type)
        | DataVarToCoordVar()
        | StoreToZarr(
            target_root=config.target_root,
            store_name=config.store_name,
            combine_dims=pattern.combine_dim_keys,
            target_chunks=dict(config.target_chunks),
            )
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
        )

    if config.num_workers > 1:
        beam_options = PipelineOptions(
                direct_num_workers=config.num_workers, direct_running_mode="multi_processing"
                )
        with beam.Pipeline(options=beam_options) as p:
            p | recipe
    else:
        with beam.Pipeline() as p:
            p | recipe

if __name__ == "__main__":
    if len(sys.argv) != 2:
       print("Usage: python scripts/convert_GEAR_beam.py <path_to_yaml_file>")
       sys.exit(1)
    
    config_file_path = sys.argv[1]
    main(config_file_path)
