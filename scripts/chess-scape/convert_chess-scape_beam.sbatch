#!/bin/bash
#SBATCH --account=fdri
#SBATCH --partition standard
#SBATCH --qos=standard
#SBATCH --time=12:00:00
#SBATCH --mem=500GB
#SBATCH -n 1
#SBATCH -o %J.out
#SBATCH -e %J.err

# Example SLURM job script for running the convert_chess-met_beam.py
# script on a SLURM-controlled HPC

# set up conda env (link to the existing one) (not sure this is strictly necessary)
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate gear

# run script, ensuring correct env is picked up
/home/users/mattjbr/miniconda3/envs/gear/bin/ipython /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/chess-met/convert_chess-met_beam.py /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/chess-met/config_chessmet.yaml
