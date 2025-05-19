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
import re
import numpy as np
import xarray as xr
import datetime as dt
import apache_beam as beam
from dateutil.relativedelta import relativedelta
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
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
def make_path(variable, time):
    #filename = config.prefix + time + config.suffix
    #filename = "chess-met_" + variable + "_gb_1km_daily_" + time + ".nc"
    #filename = f"chess-met_{variable}_gb_1km_daily_{time}.nc"
    filename = config.filename
    filename = re.sub(r"{time1}.*{time2}", "{time}", filename)
    filename = re.sub(r"{time1}", "{time}", filename)
    filename = re.sub(r"{time}", time, filename)
    filename = re.sub(r"{varname}", variable, filename)
    
    print(f"FILENAME: {filename}")
    return os.path.join(config.input_dir, filename)

freq = 'M' # monthly hard coded for now
if freq == 'M':
    # smallest unit we can have represented in the timestring
    # (one unit less than the file frequency)
    # e.g. hours for days, days for months, months for years
    delta = dt.timedelta(days=1)

# hardcoded for monthly file frequency for now
# could be generalised in the future
if "{time2}" in config.filename:
    # treat as a range
    
    if not "{time1}" in config.filename:
        raise SyntaxError("{time2} cannot be present in config.filename if " + \
                          "{time1} is not. You probably just need/meant to " + \
                          "change {time2} to {time1}")
    if config.time1 == "":
       raise SyntaxError("config.time1 cannot be a blank string")
    if config.time2 == "":
       raise SyntaxError("config.time2 cannot be a blank string")
    if not config.time1:
       raise SyntaxError("config.time1 is empty and must be specified")
    if not config.time2:
       raise SyntaxError("config.time2 is empty and must be specified")
    
    time1s = [
       dt.datetime(year, month, 1).strftime(config.time1)
       for year in range(config.start_year, config.end_year + 1)
       for month in range(1, 13)
       if not (year == config.start_year and month < config.start_month) and not (year == config.end_year and month > config.end_month)
    ]
    time2s = [
       (dt.datetime(year, month, 1)+relativedelta(months=1)-delta).strftime(config.time2)
       for year in range(config.start_year, config.end_year + 1)
       for month in range(1, 13)
       if not (year == config.start_year and month < config.start_month) and not (year == config.end_year and month > config.end_month)
    ]
    # combine into one string using re.search (& .group() on the output)
    timestring = re.search(r"{time1}.*{time2}", config.filename).group() # the timestring 
    ranges = []
    # replace {time1} and {time2} with what was determined for time1s and time2s
    for t in range(len(time1s)):
        range1 = re.sub(r"{time1}", time1s[t], timestring)
        range2 = re.sub(r"{time2}", time2s[t], range1)
        ranges.append(range2)
    logging.info(ranges)
    times = ranges
else:
    # treat as a timestamp
    if config.time1 == "":
        raise SyntaxError("config.time1 cannot be a blank string")
    if not config.time1:
        raise SyntaxError("config.time1 is empty and must be specified")
    times = [
        dt.datetime(year, month, 1).strftime(config.time1)
        for year in range(config.start_year, config.end_year + 1)
        for month in range(1, 13)
        if not (year == config.start_year and month < config.start_month) and not (year == config.end_year and month > config.end_month)
    ]


time_concat_dim = ConcatDim("time", times)
var_merge_dim = MergeDim("variable", config.varnames)

pattern = FilePattern(make_path, time_concat_dim, var_merge_dim)
if config.prune > 0:
    pattern = pattern.prune(nkeep=config.prune)

for item in pattern.items():
    logging.info(item)
    
if config.overwrites == "on":
    if config.overwrite_source:
        owfile = os.path.join(config.input_dir, config.overwrite_source)
        try:
            owds = xr.open_dataset(owfile)
        except FileNotFoundError:
            raise FileNotFoundError("File to be used for overwriting " + owfile + " does not exist")
    else:
        owfile = make_path(config.varnames[-1], times[-1])
        try:
            owds = xr.open_dataset(owfile)
        except FileNotFoundError:
            raise FileNotFoundError("File to be used for overwriting " + owfile + " does not exist. \n" + \
                                    "This is likely because the generated filenames are incorrect.")
        
# which dimensions we are chunking over:
chunkdims = list(dict(config.target_chunks).keys())
# how many dimensions we are chunking over:
nchunkdims = len(chunkdims)
# which dimensions we are concatenating (combining source files) over:
concdims = pattern.concat_dims

# =============================================================================
# Define our preprocessing functions
#
# Add in our own custom Beam PTransform (Parallel Transform) to apply
# some preprocessing to the dataset. In the first case to replace the values
# of some of the (auxillary?) coordinate variables in each file with the values
# from one specific file in the dataset. To avoid issues that might 
# arise when the dataset is updated with a new year(s) of data and as a side 
# effect has very very slightly different coordinate values.

# They are implemented as subclasses of the beam.PTransform class
class CoordVarOverwrite(beam.PTransform):

    # not sure why it needs to be a staticmethod
    @staticmethod
    # preprocess functions should take in and return an
    # object of type Indexed[T]. These are pangeo-forge-recipes
    # derived types, internal to the functioning of the
    # pangeo-forge-recipes transforms.
    # They consist of a list of 2-item tuples,
    # each containing some type of 'index' and a 'chunk' of
    # the dataset or a reference to it, as can be seen in
    # the first line of the function below
    def _coordvar_overwrite(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        # do something to each ds chunk here 
        # and leave index untouched.

        # are we doing any variable overwriting?:
        if config.overwrites == "on":
            
            # has the user specified which variables to overwrite?
            # If not, default to all variables which don't contain dimensions
            # we are concatenating over and are not the variables for the 
            # dimensions we're chunking over. 
            if not config.var_overwrites:
                
                vars_to_replace = []
                # go through all the variables in the dataset 'ds'
                # to figure out which we can safely meddle with.
                for key in ds.variables.keys():
                    ndims = len(ds[key].shape)
                    if ndims != nchunkdims: # rule out the main dataset variables
                        # rule out the variables that make use of the concat dims
                        concdimmatches = [concdim in ds[key].dims for concdim in concdims]
                        matches_bool = np.any(concdimmatches)
                        # and rule out vars that are the coords for the chunking dims
                        if key not in chunkdims and not matches_bool:
                            logging.info('Adding ' + key + ' to list of vars to replace')
                            vars_to_replace.append(key)
            else:
                # if user has specified variables to overwrite, just use them
                vars_to_replace = list(config.var_overwrites)
                
            # do the coord value replacement for the variables that remain
            for vari in vars_to_replace:
                logging.info('Replacing values of ' + vari + ' with those from ' + owfile)
                try:
                   ds[vari].values = owds[vari].values
                except KeyError:
                   raise KeyError("Variable " + vari + " not present in overwrite file " + owfile)
        
        return index, ds

    # this expand function is a necessary part of
    # developing your own Beam PTransforms, I think
    # it wraps the above preprocess function and applies
    # it to the PCollection, i.e. all the 'ds' chunks in Indexed
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._coordvar_overwrite)
    
# In this case to convert any coordinate variables that show up as *data*
# variables in the xarray dataset model, such as the 'bounds' variables, 
# to *coordinate* variables so that pangeo-forge-recipes leaves them alone
class DataVarToCoordVar(beam.PTransform):
    
    @staticmethod
    def _datavar_to_coordvar(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        
        # Here we convert some of the variables in the file
        # to coordinate variables so that pangeo-forge-recipes
        # can process them. These are variables that show up as *data*
        # variables but should really be *coord* variables
        vars_to_coord = []
        for key in ds.data_vars.keys(): # go through all the *data* variables
            ndims = len(ds[key].shape)
            if ndims != nchunkdims: # rule out the main dataset variable(s)
                if key not in chunkdims: # rule out the coord vars chunked over
                    logging.info('Converting ' + key + ' to coordinate variable')
                    vars_to_coord.append(key)
        ds = ds.set_coords(vars_to_coord)    

        return index, ds
    
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._datavar_to_coordvar)

# =============================================================================
# Assemble the recipe (workflow) we want to run from the various building
# blocks we have from pangeo-forge-recipes and our own preprocess building
# blocks defined above

recipe = (
        beam.Create(pattern.items())
        | OpenWithXarray(file_type=pattern.file_type)
        | CoordVarOverwrite()
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
