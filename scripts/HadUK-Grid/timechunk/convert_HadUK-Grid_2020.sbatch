#!/bin/bash
#SBATCH --account=fdri
#SBATCH --partition standard
#SBATCH --qos=short
#SBATCH --time=00:20:00
#SBATCH --mem=2GB
#SBATCH -n 1
#SBATCH -o %J.out
#SBATCH -e %J.err

# Example SLURM job script for running the conversion scripts for wrf data
# on a SLURM-controlled HPC

# ensure uv is picked up
## CHANGE TO THE DIRECTORY SHOWN WHEN RUNNING 'which uv' ##
export PATH=/home/users/mattjbr/.local/bin:$PATH

# move into the directory containing the code
## CHANGE TO THE DIRECTORY THAT CONTAINS THE CODE SCRIPTS FOLDER ##
cd /gws/ssde/j25b/fdri/users/matbro/dri_gridded_data/scripts

# run
uv run convert.py ../config/HadUK-Grid/timechunk/HadUK-Grid_2020.yaml
