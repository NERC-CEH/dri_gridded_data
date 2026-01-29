#!/bin/bash
#SBATCH --account=fdri
#SBATCH --partition standard
#SBATCH --qos=high
#SBATCH --time=23:59:00
#SBATCH --mem=499GB
#SBATCH -n 16
#SBATCH -o %J.out
#SBATCH -e %J.err

# Example SLURM job script for running the conversion scripts for wrf data
# on a SLURM-controlled HPC

# ensure uv is picked up
## CHANGE TO THE DIRECTORY SHOWN WHEN RUNNING 'which uv' ##
export PATH=/home/users/mattjbr/.local/bin:$PATH

# move into the directory containing the code
## CHANGE TO THE DIRECTORY THAT CONTAINS THE CODE SCRIPTS FOLDER ##
cd /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts

# run
uv run convert.py wrf ../config/wrf_config.yaml
