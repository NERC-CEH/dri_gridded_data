import argparse
from dri_gridded_data.chessmet.converter import convert_chessmet
from dri_gridded_data.utils import load_yaml_config

parser = argparse.ArgumentParser()
parser.add_argument("configpath", type=str, help="Path to the configuration file")

args, beam_args = parser.parse_known_args()

config = load_yaml_config(args.configpath)

convert_chessmet(config)
