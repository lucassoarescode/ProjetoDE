"""
Configuracao e validacao de todos os parametros relevantes do pipeline ProjetoDE.
Carrega e valida o arquivo config.yml usando modelos Pydantic.
"""

from pathlib import Path
from typing import Dict, List
from pydantic import BaseModel
import yaml

# Caminhos calculados em relacao a localizacao deste arquivo (src/core.py → raiz do projeto)
PACKAGE_ROOT: Path = Path(__file__).resolve().parent.parent
ASSETS_PATH: Path = PACKAGE_ROOT / "assets"
CONFIG_FILE_PATH: Path = ASSETS_PATH / "config.yml"


class ApiConfig(BaseModel):
    """Configuracao da API randomuser.me."""

    url: str
    results: int


class DatabaseConfig(BaseModel):
    """Configuracao do banco de dados SQLite."""

    filename: str
    table: str


class AppConfig(BaseModel):
    """Objeto de configuracao principal do pipeline."""

    api: ApiConfig
    database: DatabaseConfig
    column_rename: Dict[str, str]
    selected_columns: List[str]


def create_and_validate_config(cfg_path: Path = CONFIG_FILE_PATH) -> AppConfig:
    """
    Carrega o arquivo de configuracao YAML e valida contra o schema Pydantic.

    Args:
        cfg_path: Caminho para o arquivo de configuracao YAML.

    Returns:
        Uma instancia validada de AppConfig.

    Raises:
        OSError: Se o arquivo de configuracao nao for encontrado no caminho informado.
    """
    try:
        with open(cfg_path, "r", encoding="utf-8") as conf_file:
            parsed_config = yaml.safe_load(conf_file)
    except FileNotFoundError:
        raise OSError(f"Arquivo de configuracao nao encontrado em: {cfg_path}")

    config = AppConfig(**parsed_config)
    return config


configs: AppConfig = create_and_validate_config()

if __name__ == "__main__":
    print(create_and_validate_config())
