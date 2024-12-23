#!/bin/bash
#SBATCH -p short-serial-4hr
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=2:00:00
#SBATCH --mem=40GB
#SBATCH --account=short4hr

# Example SLURM job script for running the convert_GEAR_beam.py
# script on a SLURM-controlled HPC

# set up conda env (link to the existing one) (not sure this is strictly necessary)
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate daskbeam

# run script, ensuring correct env is picked up
/home/users/mattjbr/miniconda3/envs/daskbeam/bin/ipython /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/GEAR/convert_GEAR_beam_dask.py /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/config/matbro.yaml
