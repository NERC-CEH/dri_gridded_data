from dataclasses import dataclass
from omegaconf import OmegaConf
from typing import Optional

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
    ensmems: list
    date_format: Optional[str] = "%Y%m%d"
    target_root: str
    store_name: str
    start_year: int
    end_year: int
    start_month: Optional[int] = 1
    end_month: Optional[int] = 12
    target_chunks: TargetChunks
    prune: Optional[int] = 0
    num_workers: Optional[int] = 1
    overwrites: Optional[str] = "off"
    var_overwrites: Optional[list] = []
    overwrite_source: Optional[str] = ""

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

