"""
Utility functions for the ProjetoDE data engineering pipeline.
Handles data ingestion from API, validation, and preparation/storage.
"""

import logging
import re
from typing import List

import pandas as pd
import requests
from pydantic import BaseModel, ValidationError, field_validator
from sqlalchemy import create_engine

from core import AppConfig, ASSETS_PATH



# ---------------------------------------------------------------------------
# Pydantic schema used by validation_inputs
# ---------------------------------------------------------------------------


class UserRecord(BaseModel):
    """Schema for a single user record after column renaming."""

    genero: str
    titulo: str
    primeiro_nome: str
    sobrenome: str
    email: str
    data_nascimento: str
    idade: int
    telefone: str
    cidade: str
    estado: str
    pais: str

    @field_validator("genero")
    @classmethod
    def validate_genero(cls, value: str) -> str:
        """Ensure gender is one of the expected values from the API."""
        if value not in ("male", "female"):
            raise ValueError(
                f"genero deve ser 'male' ou 'female', recebeu '{value}'"
            )
        return value

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        """Basic check that the field contains an '@' character."""
        if "@" not in value:
            raise ValueError(f"email invalido: '{value}'")
        return value

    @field_validator("idade")
    @classmethod
    def validate_idade(cls, value: int) -> int:
        """Ensure age is a positive integer."""
        if value <= 0:
            raise ValueError(f"idade deve ser positiva, recebeu {value}")
        return value

    @field_validator("telefone")
    @classmethod
    def validate_telefone(cls, value: str) -> str:
        """Ensure telephone contains only digits after cleaning."""
        if not re.fullmatch(r"\d+", value):
            raise ValueError(
                f"telefone deve conter apenas digitos apos limpeza, recebeu '{value}'"
            )
        return value


class MultipleUserSchema(BaseModel):
    """Wrapper for validating a list of user records at once."""

    inputs_raw: List[UserRecord]


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


def ingestion(configs: AppConfig) -> pd.DataFrame:
    """
    Consume data from the randomuser.me public API and return a flat DataFrame.

    Makes a GET request to the configured API URL requesting at least 10 results,
    then normalises the nested JSON structure into a tabular format.

    Args:
        configs: Validated application configuration object.

    Returns:
        A pandas DataFrame with one row per user and one column per nested field.

    Raises:
        requests.HTTPError: If the API returns a non-2xx status code.
    """
    url: str = configs.api.url
    results: int = configs.api.results

    logging.info(f"Consumindo API: {url} | resultados solicitados: {results}")

    response = requests.get(url, params={"results": results}, timeout=30)
    response.raise_for_status()

    data = response.json()["results"]
    df: pd.DataFrame = pd.json_normalize(data)

    logging.info(f"Ingestao concluida: {len(df)} registros obtidos")
    return df


def validation_inputs(df: pd.DataFrame, configs: AppConfig) -> pd.DataFrame:
    """
    Validate a prepared DataFrame against the expected schema before persisting.

    Each row is checked against the UserRecord Pydantic model. If any row fails
    validation the error details are written to the log file and a ValueError is
    raised to interrupt the pipeline. On success, a confirmation message is logged.

    Args:
        df: DataFrame that has already been renamed and cleaned by preparation().
        configs: Validated application configuration object (reserved for future use).

    Returns:
        The original DataFrame unchanged when all rows pass validation.

    Raises:
        ValueError: If one or more rows do not conform to the expected schema.
    """
    records = df.replace({float("nan"): None}).to_dict(orient="records")

    try:
        MultipleUserSchema(inputs_raw=records)
    except ValidationError as error:
        logging.error(f"Erros de validacao encontrados:\n{error}")
        raise ValueError(
            "Dados fora do padrao. Processo interrompido. Verifique o arquivo de log."
        ) from error

    logging.info("Dados corretos")
    return df


def preparation(df: pd.DataFrame, configs: AppConfig) -> None:
    """
    Transform the raw ingested DataFrame and persist it to a SQLite database.

    Steps performed:
        1. Rename columns according to the mapping in config.yml.
        2. Select only the configured subset of columns.
        3. Remove special characters from the 'telefone' column (keep digits only).
        4. Convert 'cep' (postcode) values to string to handle mixed types.
        5. Validate the cleaned data via validation_inputs().
        6. Adjust column data types (idade → int, data_nascimento → datetime).
        7. Save the final DataFrame to the configured SQLite table.

    Args:
        df: Raw DataFrame returned by ingestion().
        configs: Validated application configuration object.

    Returns:
        None. Side-effect: writes a SQLite database file under assets/.
    """
    # 1. Rename columns
    df = df.rename(columns=configs.column_rename)

    # 2. Select configured columns (ignore any that were not present in the API response)
    available_cols = [col for col in configs.selected_columns if col in df.columns]
    df = df[available_cols].copy()

    # 3. Remove special characters from phone — keep digits only
    df["telefone"] = df["telefone"].str.replace(r"[^\d]", "", regex=True)

    # 4. Validate before type conversion (data_nascimento is still a raw ISO string)
    validation_inputs(df, configs)

    # 5. Adjust data types
    df["idade"] = df["idade"].astype(int)
    df["data_nascimento"] = pd.to_datetime(df["data_nascimento"], utc=True)

    # 6. Save to SQLite
    db_path = ASSETS_PATH / configs.database.filename
    engine = create_engine(f"sqlite:///{db_path}")

    df.to_sql(configs.database.table, con=engine, if_exists="replace", index=False)

    logging.info(
        f"Dados salvos em '{db_path}' | tabela: '{configs.database.table}' "
        f"| registros: {len(df)}"
    )
