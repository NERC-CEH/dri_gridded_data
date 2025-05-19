from dataclasses import dataclass
from omegaconf import OmegaConf

@dataclass
class TargetChunks:
    time: int
    y: int
    x: int
    bnds: int

@dataclass
class Config:
    input_dir: str
    filename: str
    varnames: list
    time1: str
    time2: str
    target_root: str
    store_name: str
    start_year: int
    end_year: int
    start_month: int
    end_month: int
    target_chunks: TargetChunks
    prune: int
    num_workers: int
    overwrites: str
    var_overwrites: list
    overwrite_source: str

def load_yaml_config(file_path: str) -> Config:
    try:
        yaml_config = OmegaConf.load(file_path)
        config_dict = OmegaConf.to_container(yaml_config)
        return Config(**config_dict)
    except FileNotFoundError as e:
        print(f"File not found: {file_path}")
        raise e
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e

