import logging
import sys
from pathlib import Path

# Configura o logging ANTES de importar utils/core para que o basicConfig seja aplicado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).resolve().parent.parent / "assets" / "app.log"),
    ],
)

import utils as utils
from core import configs

if __name__ == '__main__':
    logging.info("Iniciando processo de ingestao")
    try:
        df = utils.ingestion(configs)
    except Exception as e:
        logging.error(f"Erro de ingestao de dados: {e}")
        raise
    try:
        utils.preparation(df, configs)
        logging.info("Fim do processo de ingestao")
    except Exception as e:
        logging.error(f"Erro de preparacao: {e}")
        raise
