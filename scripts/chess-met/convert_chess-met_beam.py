# MJB (UKCEH) Aug-2024
# Example script for a pangeo-forge-recipe to convert
# gridded netcdf files to a zarr datastore ready for upload
# to object storage.
# See jupyter notebook for more details and explanations/comments
# Please note that this script/notebook is intended to serve as an example only,
# and be adapted for your own datasets.

import os
import logging
import sys
import xarray as xr
import apache_beam as beam
from datetime import datetime
from calendar import monthrange
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
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

dotdotpath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.append(dotdotpath)
from GEAR_config import load_yaml_config
sys.path.remove(dotdotpath)

if len(sys.argv) != 2:
   print("Usage: python scripts/convert_chess-met_beam.py <path_to_yaml_file>")
   sys.exit(1)

file_path = sys.argv[1]
config = load_yaml_config(file_path)

logging.info('Converting data in ' + config.input_dir + ' from ' + str(config.start_year) + ' to ' + str(config.end_year))
logging.info('Outputting to ' + config.store_name + ' in ' + config.target_root)
logging.info('Rechunking to ' + str(config.target_chunks) + ' using ' + str(config.num_workers) + ' process(es)')
if config.prune > 0:
    logging.info('Only using first ' + str(config.prune) + ' files')

if not os.path.exists(config.target_root):
    os.makedirs(config.target_root)

# chess-met_tas_gb_1km_daily_20171201-20171231.nc
def make_path(time):
    filename = config.prefix + time + config.suffix
    print(f"FILENAME: {filename}")
    return os.path.join(config.input_dir, filename)

ranges = [
    f"{year:04d}{month:02d}01-{year:04d}{month:02d}{monthrange(year, month)[1]:02d}"
    for year in range(config.start_year, config.end_year + 1)
    for month in range(1, 13)
    if not (year == config.start_year and month < config.start_month) and not (year == config.end_year and month > config.end_month)
]
logging.info(ranges)

time_concat_dim = ConcatDim("time", ranges)

pattern = FilePattern(make_path, time_concat_dim)
if config.prune > 0:
    pattern = pattern.prune(nkeep=config.prune)

for item in pattern.items():
    logging.info(item)
    
lastfile = make_path(ranges[-1])
lastds = xr.open_dataset(lastfile)

# Add in our own custom Beam PTransform (Parallel Transform) to apply
# some preprocessing to the dataset. In this case to convert the
# 'bounds' variables to coordinate rather than data variables, so
# that pangeo-forge-recipes leaves them alone

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
        print(index)
        print(ds) 
        # do something to each ds chunk here 
        # and leave index untouched.
        # Here we convert some of the variables in the file
        # to coordinate variables so that pangeo-forge-recipes
        # can process them
        vars_to_coord = []
        chunkdims = list(dict(config.target_chunks).keys())
        nchunkdims = len(chunkdims)
        concdim = pattern.concat_dims[0]
        logging.info('chunkdims: ' + str(chunkdims))
        logging.info('nchunkdims: ' + str(nchunkdims))
        
        vars_to_replace = []
        for key in ds.variables.keys():
            ndims = len(ds[key].shape)
            logging.info('Var ' + key + ' has ' + str(ndims) + ' dims')
            if ndims != nchunkdims:
                if key not in chunkdims and concdim not in key:
                    logging.info('Adding ' + key + ' to list of vars to replace')
                    vars_to_replace.append(key)
         
        for vari in vars_to_replace:
            logging.info('Replacing values of ' + vari + ' with those from ' + lastfile)            
            ds[vari].values = lastds[vari].values
            
        for key in ds.data_vars.keys():
            ndims = len(ds[key].shape)
            if ndims != nchunkdims:
                if key not in chunkdims:
                    logging.info('Converting ' + key + ' to coordinate variable')
                    vars_to_coord.append(key)
        ds = ds.set_coords(vars_to_coord)
        

        
        return index, ds

    # this expand function is a necessary part of
    # developing your own Beam PTransforms, I think
    # it wraps the above preprocess function and applies
    # it to the PCollection, i.e. all the 'ds' chunks in Indexed
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._datavar_to_coordvar)

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

logging.info('Executing pipeline...')
if config.num_workers > 1:
    beam_options = PipelineOptions(
            direct_num_workers=config.num_workers, direct_running_mode="multi_processing", auto_unique_labels=True,
    )
    with beam.Pipeline(options=beam_options) as p:
       p | recipe
else:
   beam_options = PipelineOptions(
      auto_unique_labels=True,
   )
   with beam.Pipeline(options=beam_options) as p:
      p | recipe
