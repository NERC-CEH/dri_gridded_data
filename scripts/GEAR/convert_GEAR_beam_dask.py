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
import apache_beam as beam
import dask_gateway
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dask.dask_runner import DaskRunner
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

from GEAR_config import load_yaml_config

if len(sys.argv) != 2:
   print("Usage: python scripts/convert_GEAR_beam.py <path_to_yaml_file>")
   sys.exit(1)

file_path = sys.argv[1]
config = load_yaml_config(file_path)


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
        # do something to each ds chunk here 
        # and leave index untouched.
        # Here we convert some of the variables in the file
        # to coordinate variables so that pangeo-forge-recipes
        # can process them
        logging.info(f'Dataset chunk before preprocessing: {ds =}')
        ds = ds.set_coords(['x_bnds', 'y_bnds', 'time_bnds', 'crs'])
        logging.info(f'Dataset chunk after preprocessing: {ds =}')
        return index, ds

    # this expand function is a necessary part of
    # developing your own Beam PTransforms, I think
    # it wraps the above preprocess function and applies
    # it to the PCollection, i.e. all the 'ds' chunks in Indexed
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._datavar_to_coordvar)
    


logging.info('Converting data in ' + config.input_dir + ' from ' + str(config.start_year) + ' to ' + str(config.end_year))
logging.info('Outputting to ' + config.store_name + ' in ' + config.target_root)
logging.info('Rechunking to ' + str(config.target_chunks) + ' using ' + str(config.num_workers) + ' process(es)')
if config.prune > 0:
    logging.info('Only using first ' + str(config.prune) + ' files')

if not os.path.exists(config.target_root):
    os.makedirs(config.target_root)

def make_path(time):
    filename = config.prefix + time + config.suffix
    print(f"FILENAME: {filename}")
    return os.path.join(config.input_dir, filename)

years = list(range(config.start_year, config.end_year + 1))
months = list(range(config.start_month, config.end_month + 1))
ymonths = [f"{year}{month:02d}" for year in years for month in months]
time_concat_dim = ConcatDim("time", ymonths)

pattern = FilePattern(make_path, time_concat_dim)
if config.prune > 0:
    pattern = pattern.prune(nkeep=config.prune)


def run_pipeline():
    if config.num_workers > 1:
        logging.info('Creating/connecting to dask cluster')
        # Connect to JASMIN Dask Gateway here somehow, read docs
        # https://help.jasmin.ac.uk/docs/interactive-computing/dask-gateway/
    
        # requires API key in config file in home dir
        #gw = dask_gateway.Gateway("https://dask-gateway.jasmin.ac.uk")
    
        # set some dask-specific options
        #options = gw.cluster_options()
        # number of cores for each worker
        #options.worker_cores = 1
        # memory in GB for each worker, default is 8GB
        #options.worker_memory = 8.0
        # to ensure the client, scheduler and workers all use the same python environment.
        # We will need to setup an environment wherever this is ultimately deployed
        #options.worker_setup = "source /home/users/mattjbr/miniconda3/bin/activate /home/users/mattjbr/miniconda3/envs/daskbeam"
    
        # create the cluster
        #cluster = gw.new_cluster(options, shutdown_on_close=False)
        # set the minimum/maximum number of workers - I imagine this will need to match
        # the number of workers we configure Beam with
        #cluster.adapt(minimum=1, maximum=config.num_workers)
    
        # get dashboard link to monitor progress
        #logging.info('Dask Dashboard link: ' + cluster.dashboard_link + '\n\n')

        dask_worker_setup_cmd = "source /home/users/mattjbr/miniconda3/bin/activate /home/users/mattjbr/miniconda3/envs/daskbeam"
        
    # the main pipeline/recipe
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
                ["--dask_gateway", "https://dask-gateway.jasmin.ac.uk",
                 "--dask_worker_cores", "4",
                 "--dask_worker_memory", "20.0",
                 "--dask_worker_setup", dask_worker_setup_cmd,
                 "--dask_workers", str(config.num_workers)], 
        )
        with beam.Pipeline(runner=DaskRunner(), options=beam_options) as p:
           p | recipe
    else:
       beam_options = PipelineOptions(
          auto_unique_labels=True,
       )
       with beam.Pipeline(options=beam_options) as p:
          p | recipe

if __name__ == "__main__":
    run_pipeline()
