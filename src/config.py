import json
from pydantic import BaseModel, ConfigDict
from pathlib import Path
from typing import Any, List, Dict

CONFIG_DIR = Path.cwd() / "configs" 
CONFIG_FILE = CONFIG_DIR / "config.json"
DEFAULT_DATA_DIR = Path.cwd() / "data"
DEFAULT_LOG_DIR = Path.cwd() / "logs"

class Config(BaseModel):
    data_dir: str = str(DEFAULT_DATA_DIR)
    log_dir: str = str(DEFAULT_LOG_DIR)
    database_name:str = "strava.db"
    per_page: int = 200
    stream_types: List[str] = [
        "time", "distance", "altitude", "velocity_smooth",
        "heartrate", "cadence", "grade_smooth"
    ]

    model_config = ConfigDict(extra="allow")

def load_config(config_path: Path = CONFIG_FILE):
    if not config_path.exists():
        cfg = Config()
        save_config(cfg.model_dump(),config_path)
        return cfg
    with open(config_path, "r") as file:
        user_config = json.load(file)
    cfg = Config(**user_config)
    merged = {**cfg.model_dump(), **{ k: v for k,v in user_config.items() if k not in cfg.model_dump().keys()}}
    if cfg.model_dump() != user_config:
        save_config(cfg.model_dump(),config_path) 
    return cfg

def save_config(config: dict[str,Any],config_path: Path = CONFIG_FILE):
    cfg = Config(**config)
    if not CONFIG_DIR.exists():
        CONFIG_DIR.mkdir(exist_ok=True,parents=True)
    merged = {**config, **cfg.model_dump()}
    with open(config_path, "w") as file:
        json.dump(merged,file)
