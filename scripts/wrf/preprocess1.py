import xarray as xr
import numpy as np
import argparse
import os
import sys
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# read in command line args, currently just the path to the config file
parser = argparse.ArgumentParser()
parser.add_argument('configpath', type=str)
        
args, beam_args = parser.parse_known_args()
file_path = args.configpath

dotdotpath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.append(dotdotpath)
from GEAR_config import load_yaml_config
sys.path.remove(dotdotpath)

config = load_yaml_config(file_path)

if not os.path.exists(config.target_root):
    os.makedirs(config.target_root)
    
files = os.listdir(config.input_dir)
keepvars = config.varnames

for infile in files:
    logging.info('Removing unwanted vars from ' + infile)
    
    infile2 = os.path.join(config.input_dir, infile)
    ds = xr.open_dataset(infile2)
    
    # drop unwanted vars
    dropvars = [var for var in ds.data_vars.keys() if not var in keepvars]
    ds_trimmed = ds.drop_vars(dropvars)
    
    # drop dimensions that are now unnecessary
    allvardims = [dim for varlist in [ds[var].dims for var in ds_trimmed.data_vars.keys()] for dim in varlist]
    allvardims = list(np.unique(np.asarray(allvardims)))
    dimstodrop = [dim for dim in ds_trimmed.dims.keys() if not dim in allvardims]
    ds_trimmed = ds_trimmed.drop_dims(dimstodrop)
    
    outpath = os.path.join(config.target_root, infile)
    logging.info('Saving to ' + outpath)
    ds_trimmed.to_netcdf(outpath)