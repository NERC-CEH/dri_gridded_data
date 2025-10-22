from dataclasses import dataclass, field
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
    target_root: str
    store_name: str
    start_year: int
    end_year: int
    target_chunks: TargetChunks
    filename: Optional[str] = ""
    varnames: Optional[list] = field(default_factory=list)
    date_format: str = "%Y%m%d"
    start_month: int = 1
    end_month: int = 12
    prune: int = 0
    num_workers: int = 1
    overwrites: Optional[str] = "off"
    var_overwrites: Optional[list] = field(default_factory=list)
    overwrite_source: Optional[str] = ""
    prefix: Optional[str] = ""
    suffix: Optional[str] = ""


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
