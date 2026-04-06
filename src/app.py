import utils as utils
import logging
from core import configs

logging.basicConfig(level=logging.INFO)

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
