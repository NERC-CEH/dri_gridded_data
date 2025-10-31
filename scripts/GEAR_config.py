from dataclasses import dataclass,field
from omegaconf import OmegaConf
from typing import Optional, Dict

#@dataclass
#class TargetChunks:
#    time: int
#    y: int
#    x: int
#    bnds: int

@dataclass
class Config:
    input_dir: str
    filename: str
    varnames: list
    target_root: str
    store_name: str
    start_year: int
    end_year: int
    skipdates: list
    target_chunks: Dict[str, int]
    concatdim: Optional[str] = "time"
    concatvar: Optional[str] = 'time'
    file_type: Optional[str] = "netcdf4"
    frequency: Optional[str] = "M"
    date_format: Optional[str] = "%Y%m%d"    
    start_month: Optional[int] = 1
    end_month: Optional[int] = 12
    prune: Optional[int] = 0
    num_workers: Optional[int] = 1
    overwrites: Optional[str] = "off"
    var_overwrites: Optional[list] = field(default_factory=lambda: [])
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

