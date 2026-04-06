"""
Configuration and validation of all relevant parameters for the ProjetoDE pipeline.
Loads and validates the config.yml file using Pydantic models.
"""

from pathlib import Path
from typing import Dict, List
from pydantic import BaseModel
import yaml

# Paths computed relative to this file's location (src/core.py → project root)
PACKAGE_ROOT: Path = Path(__file__).resolve().parent.parent
ASSETS_PATH: Path = PACKAGE_ROOT / "assets"
CONFIG_FILE_PATH: Path = ASSETS_PATH / "config.yml"


class ApiConfig(BaseModel):
    """Configuration for the randomuser.me API."""

    url: str
    results: int


class DatabaseConfig(BaseModel):
    """Configuration for the SQLite database."""

    filename: str
    table: str


class AppConfig(BaseModel):
    """Master configuration object for the pipeline."""

    api: ApiConfig
    database: DatabaseConfig
    column_rename: Dict[str, str]
    selected_columns: List[str]


def create_and_validate_config(cfg_path: Path = CONFIG_FILE_PATH) -> AppConfig:
    """
    Load the YAML config file and validate it against the Pydantic schema.

    Args:
        cfg_path: Path to the YAML configuration file.

    Returns:
        A validated AppConfig instance.

    Raises:
        OSError: If the config file is not found at the given path.
    """
    try:
        with open(cfg_path, "r", encoding="utf-8") as conf_file:
            parsed_config = yaml.safe_load(conf_file)
    except FileNotFoundError:
        raise OSError(f"Config file not found at: {cfg_path}")

    config = AppConfig(**parsed_config)
    return config


configs: AppConfig = create_and_validate_config()

if __name__ == "__main__":
    print(create_and_validate_config())
