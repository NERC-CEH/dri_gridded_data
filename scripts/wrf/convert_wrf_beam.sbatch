#!/bin/bash
#SBATCH --account=fdri
#SBATCH --partition standard
#SBATCH --qos=high
#SBATCH --time=22:59:00
#SBATCH --mem=498GB
#SBATCH -n 16
#SBATCH -o %J.out
#SBATCH -e %J.err

# Example SLURM job script for running the convert_wrf_beam.py
# script on a SLURM-controlled HPC

# set up conda env (link to the existing one) (not sure this is strictly necessary)
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate gear

# run script, ensuring correct env is picked up
/home/users/mattjbr/miniconda3/envs/gear/bin/ipython /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/wrf/convert_wrf_beam.py /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/wrf/config_wrf.yaml
