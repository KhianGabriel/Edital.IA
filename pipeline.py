"""Orquestrador do pipeline completo.

Roda em sequência: crawler → conversor → validador → vetorização → extração.
Pode ser executado manualmente (`python pipeline.py`) ou via agendador
(Windows Task Scheduler / cron) sem interação humana.

Persiste estatísticas da execução em state/pipeline_state.json.
"""
import os
import logging
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from crawler import executar_crawler
from pymu_conversor_markdown import converter_pdfs
from validador import filtrar_invalidos
from vector_store import popular_banco
from extrator_rag import extrair_todos
from state import salvar_pipeline_state

load_dotenv(".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Quantos anos para tras o crawler vai buscar (1 = apenas o ano corrente).
# Configuravel via .env: ANOS_LIMITE=N
ANOS_LIMITE_DEFAULT = int(os.getenv("ANOS_LIMITE", "1"))


def executar_pipeline(anos_limite: int = ANOS_LIMITE_DEFAULT) -> dict:
    """Executa o pipeline completo. Retorna dict com estatísticas."""
    inicio = time.time()
    log.info("==================== PIPELINE INICIADO ====================")
    log.info(f"anos_limite = {anos_limite}")

    stats: dict = {
        "iniciado_em": datetime.now().isoformat(timespec="seconds"),
        "anos_limite": anos_limite,
    }

    try:
        log.info(">>> [1/5] Crawler")
        stats["crawler"] = executar_crawler(anos_limite=anos_limite)

        log.info(">>> [2/5] Conversor PDF -> Markdown")
        mds = converter_pdfs()
        stats["conversor"] = {"total_mds": len(mds)}

        log.info(">>> [3/5] Validador")
        validos, invalidos = filtrar_invalidos(Path("editais_markdown"))
        stats["validador"] = {"validos": len(validos), "invalidos": len(invalidos)}

        log.info(">>> [4/5] Vector store (chunking + embedding)")
        popular_banco()

        log.info(">>> [5/5] Extrator (pré-extração para o front)")
        extrair_todos()

        stats["status"] = "sucesso"
        stats["erro"] = None
    except Exception as e:
        log.exception("Pipeline falhou")
        stats["status"] = "falha"
        stats["erro"] = str(e)
    finally:
        stats["duracao_segundos"] = round(time.time() - inicio, 2)
        salvar_pipeline_state(stats)
        log.info(
            f"==================== PIPELINE FINALIZADO em "
            f"{stats['duracao_segundos']}s ===================="
        )

    return stats


if __name__ == "__main__":
    executar_pipeline()
