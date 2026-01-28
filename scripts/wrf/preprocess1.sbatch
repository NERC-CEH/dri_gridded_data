#!/bin/bash
#SBATCH --account=fdri
#SBATCH --partition standard
#SBATCH --qos=short
#SBATCH --time=3:59:59
#SBATCH --mem=32GB
#SBATCH -n 1
#SBATCH -o %J.out
#SBATCH -e %J.err

# set up conda env (link to the existing one) (not sure this is strictly necessary)
export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate gear

# run script, ensuring correct env is picked up
/home/users/mattjbr/miniconda3/envs/gear/bin/ipython /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/wrf/preprocess1.py /gws/nopw/j04/fdri/users/matbro/dri_gridded_data/scripts/wrf/config_wrf.yaml
