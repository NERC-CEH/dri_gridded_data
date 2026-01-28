import argparse
from dri_gridded_data.chessmet.converters import convert_chessmet
from dri_gridded_data.gear.converters import convert_gear_hourly
from dri_gridded_data.utils import load_yaml_config

parser = argparse.ArgumentParser()
parser.add_argument(
    "dataset", choices=["chess", "gearh"], help="Dataset type to convert"
)
parser.add_argument("configpath", type=str, help="Path to the configuration file")

args, beam_args = parser.parse_known_args()

config = load_yaml_config(args.configpath)

converters = {"chess": convert_chessmet, "gearh": conver_gear_hourly}

convert_func = converters[args.dataset]
convert_func(config)
