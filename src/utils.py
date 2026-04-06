"""
Funcoes utilitarias do pipeline de engenharia de dados do ProjetoDE.
Responsavel pela ingestao de dados da API, validacao e preparacao/armazenamento.
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
# Schema Pydantic utilizado pela funcao validation_inputs
# ---------------------------------------------------------------------------


class UserRecord(BaseModel):
    """Schema para um registro de usuario apos renomeacao das colunas."""

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
        """Verifica se o genero e um dos valores esperados pela API."""
        if value not in ("male", "female"):
            raise ValueError(
                f"genero deve ser 'male' ou 'female', recebeu '{value}'"
            )
        return value

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        """Verifica basicamente se o campo contem o caractere '@'."""
        if "@" not in value:
            raise ValueError(f"email invalido: '{value}'")
        return value

    @field_validator("idade")
    @classmethod
    def validate_idade(cls, value: int) -> int:
        """Verifica se a idade e um inteiro positivo."""
        if value <= 0:
            raise ValueError(f"idade deve ser positiva, recebeu {value}")
        return value

    @field_validator("telefone")
    @classmethod
    def validate_telefone(cls, value: str) -> str:
        """Verifica se o telefone contem apenas digitos apos a limpeza."""
        if not re.fullmatch(r"\d+", value):
            raise ValueError(
                f"telefone deve conter apenas digitos apos limpeza, recebeu '{value}'"
            )
        return value


class MultipleUserSchema(BaseModel):
    """Objeto agrupador para validar uma lista de registros de usuarios de uma vez."""

    inputs_raw: List[UserRecord]


# ---------------------------------------------------------------------------
# Funcoes do pipeline
# ---------------------------------------------------------------------------


def ingestion(configs: AppConfig) -> pd.DataFrame:
    """
    Consome dados da API publica randomuser.me e retorna um DataFrame plano.

    Realiza uma requisicao GET para a URL configurada solicitando ao menos 10 resultados,
    em seguida normaliza a estrutura JSON aninhada para formato tabular.

    Args:
        configs: Objeto de configuracao validado da aplicacao.

    Returns:
        DataFrame pandas com uma linha por usuario e uma coluna por campo aninhado.

    Raises:
        requests.HTTPError: Se a API retornar um status code diferente de 2xx.
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
    Valida um DataFrame preparado contra o schema esperado antes de persistir.

    Cada linha e verificada contra o modelo Pydantic UserRecord. Se alguma linha falhar
    na validacao, os detalhes do erro sao gravados no arquivo de log e um ValueError e
    lancado para interromper o pipeline. Em caso de sucesso, uma mensagem de confirmacao e registrada.

    Args:
        df: DataFrame ja renomeado e limpo pela funcao preparation().
        configs: Objeto de configuracao validado da aplicacao (reservado para uso futuro).

    Returns:
        O DataFrame original sem alteracoes quando todas as linhas passam na validacao.

    Raises:
        ValueError: Se uma ou mais linhas nao estiverem em conformidade com o schema esperado.
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
    Transforma o DataFrame bruto ingerido e persiste em um banco de dados SQLite.

    Etapas realizadas:
        1. Renomeia colunas conforme o mapeamento definido no config.yml.
        2. Seleciona apenas o subconjunto de colunas configurado.
        3. Remove caracteres especiais da coluna 'telefone' (mantem apenas digitos).
        4. Valida os dados limpos via validation_inputs().
        5. Ajusta os tipos das colunas (idade -> int, data_nascimento -> datetime).
        6. Salva o DataFrame final na tabela SQLite configurada.

    Args:
        df: DataFrame bruto retornado pela funcao ingestion().
        configs: Objeto de configuracao validado da aplicacao.

    Returns:
        None. Efeito colateral: grava um arquivo de banco SQLite em assets/.
    """
    # 1. Renomeia as colunas
    df = df.rename(columns=configs.column_rename)

    # 2. Seleciona as colunas configuradas (ignora as que nao estiverem na resposta da API)
    available_cols = [col for col in configs.selected_columns if col in df.columns]
    df = df[available_cols].copy()

    # 3. Remove caracteres especiais do telefone — mantem apenas digitos
    df["telefone"] = df["telefone"].str.replace(r"[^\d]", "", regex=True)

    # 4. Valida antes da conversao de tipos (data_nascimento ainda e string ISO bruta)
    validation_inputs(df, configs)

    # 5. Ajusta os tipos de dados
    df["idade"] = df["idade"].astype(int)
    df["data_nascimento"] = pd.to_datetime(df["data_nascimento"], utc=True)

    # 6. Salva no SQLite
    db_path = ASSETS_PATH / configs.database.filename
    engine = create_engine(f"sqlite:///{db_path}")

    df.to_sql(configs.database.table, con=engine, if_exists="replace", index=False)

    logging.info(
        f"Dados salvos em '{db_path}' | tabela: '{configs.database.table}' "
        f"| registros: {len(df)}"
    )
