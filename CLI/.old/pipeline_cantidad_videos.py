import argparse
import asyncio
import os
import sys
import datetime
import json
from typing import Iterable

import aiosqlite
from pyrogram import enums
from pyrogram.errors import RPCError

"""
Pipeline orquestado para refrescar métricas de videos:
1) Contar videos reales en la nube por canal/grupo.
2) Recalcular indexados desde la BD.
3) Calcular duplicados (conteo simple por file_unique_id).
4) Sincronizar faltantes hasta un tope de nuevos indexados.
5) Indexar histórico hacia atrás.
6) Recalcular indexados.
7) Recalcular duplicados.
"""

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config import DB_PATH  # noqa: E402
from services.telegram_client import get_client  # noqa: E402
from database.videos import (  # noqa: E402
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
    db_count_videos_by_chat,
)
from database.chats import db_upsert_chat_video_count  # noqa: E402
from database import init_db  # noqa: E402
from utils import log_timing


# Reuso de scripts existentes
from CLI.cantidad_videos.auditar_conteo_videos_chats_paralelo import (  # noqa: E402
    contar_videos_en_todos_mis_chats_paralelo,
)
from CLI.cantidad_videos.recalcular_indexados_desde_bd import (  # noqa: E402
    main as recalcular_indexados_main,
)
from CLI.cantidad_videos.indexador_historico import (  # noqa: E402
    obtener_chats_con_historial,
    escanear_historia_antigua,
)
from CLI.cantidad_videos.sincronizador_con_stop import (  # noqa: E402
    sync_con_stop,
)
from CLI.cantidad_videos.guardar_chats import guardar_chats  # noqa: E402
from CLI.optimizar_indices_db import crear_indices_optimizados  # noqa: E402

RUN_TS = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
REPORT_PATH = os.path.join(ROOT_DIR, f"pipeline_report_{RUN_TS}.json")
REPORT_ENTRIES: list[dict] = []

Paso0=True  # Desactivado por defecto para velocidad - activar con --guardar-chats

def _add_report(step: str, status: str, data: dict | None = None) -> None:
    entry = {
        "step": step,
        "status": status,
        "ts": datetime.datetime.utcnow().isoformat(),
    }
    if data:
        entry["data"] = data
    REPORT_ENTRIES.append(entry)

def _write_report() -> None:
    """Escribe el reporte completo al disco (llamar solo al final)."""
    try:
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {"run_ts": RUN_TS, "report_path": REPORT_PATH, "entries": REPORT_ENTRIES},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except Exception as e:
        log_timing(f"[WARN] No se pudo escribir reporte {REPORT_PATH}: {e}")


# ---------------------------------------------------------------------------
# Helpers SQL
# ---------------------------------------------------------------------------
async def contar_duplicados_y_actualizar() -> None:
    """
    Cuenta duplicados por chat (file_unique_id con count>1) y actualiza chat_video_counts. 
    No borra ni marca, solo escribe el número de duplicados.
    Optimizado: usa una sola query con LEFT JOIN para actualizar todos los chats.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Primero resetear todos a 0
        await db.execute("UPDATE chat_video_counts SET duplicados = 0")
        
        # Luego actualizar solo los que tienen duplicados con una query optimizada
        query = """
            UPDATE chat_video_counts
            SET duplicados = (
                SELECT COUNT(*)
                FROM (
                    SELECT vm.video_id
                    FROM video_messages vm
                    JOIN chats c ON vm.chat_id = c.chat_id
                    WHERE vm.chat_id = chat_video_counts.chat_id
                      AND COALESCE(c.is_owner, 0) = 0
                    GROUP BY vm.video_id
                    HAVING COUNT(*) > 1
                )
            )
            WHERE EXISTS (
                SELECT 1 FROM video_messages vm
                WHERE vm.chat_id = chat_video_counts.chat_id
            )
        """
        await db.execute(query)
        await db.commit()


async def recalcular_indexados_async() -> None:
    """
    Llama al recalculador síncrono en un hilo para poder usar await.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, recalcular_indexados_main)


async def contar_videos_nube_async() -> None:
    await contar_videos_en_todos_mis_chats_paralelo()


async def indexar_historico_async(max_chats: int | None = None) -> None:
    client = get_client(clone_for_cli=True)
    await client.start()
    try:
        chats: Iterable[int] = await obtener_chats_con_historial()
        if max_chats:
            chats = list(chats)[:max_chats]

        for chat_id in chats:
            try:
                await escanear_historia_antigua(client, chat_id)
            except Exception as e:
                log_timing(f"[WARN] Error escaneando histórico {chat_id}: {e}")
    finally:
        try:
            await client.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
async def run_pipeline(args: argparse.Namespace) -> None:
    await init_db()
    
    # Optimizar índices de BD antes de comenzar (solo una vez)
    log_timing("\n=== Paso -1: Optimizar índices de base de datos ===")
    log_timing("Iniciando optimización de índices")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, crear_indices_optimizados)
    _add_report("paso_minus1_optimizar_indices", "ok")
    log_timing("Terminando optimización de índices")
    
    if Paso0:
        log_timing("\n=== Paso 0: Guardar/actualizar chats ===")
        log_timing("Iniciando paso 0")
        if args.dry_run:
            log_timing("Saltado (dry-run).")
            _add_report("paso0_guardar_chats", "skipped", {"reason": "dry_run"})
        else:
            await guardar_chats(limit=None)
            _add_report("paso0_guardar_chats", "ok")
        log_timing("Terminando paso 0")
    else:
        log_timing("\n=== Paso 0: Guardar/actualizar chats (OMITIDO) ===")
        log_timing("Paso 0 omitido para velocidad. Usa --guardar-chats para ejecutarlo.")
        _add_report("paso0_guardar_chats", "skipped", {"reason": "not_requested"})

    log_timing("\n=== Paso 1: Contar videos en la nube ===")
    log_timing("Iniciando paso 1")
    if args.dry_run:
        log_timing("Saltado (dry-run).")
        _add_report("paso1_contar_videos_nube", "skipped", {"reason": "dry_run"})
    else:
        await contar_videos_nube_async()
        _add_report("paso1_contar_videos_nube", "ok")
    log_timing("Terminando paso 1")

    log_timing("\n=== Paso 2: Recalcular indexados desde BD ===")
    log_timing("Iniciando paso 2")
    await recalcular_indexados_async()
    _add_report("paso2_recalcular_indexados", "ok")
    log_timing("Terminando paso 2")

    log_timing("\n=== Paso 3: Calcular duplicados ===")
    log_timing("Iniciando paso 3")
    await contar_duplicados_y_actualizar()
    _add_report("paso3_duplicados", "ok")
    log_timing("Terminando paso 3")

    log_timing(f"\n=== Paso 4: Sincronizar faltantes (detener tras {args.max_nuevos} consecutivos existentes) ===")
    log_timing("Iniciando paso 4")
    if args.dry_run:
        log_timing("Saltado (dry-run).")
        _add_report("paso4_sync_faltantes", "skipped", {"reason": "dry_run"})
    else:
        await sync_con_stop(
            max_chats=args.max_chats,
            only_chat_id=None,
        )
        log_timing("Sincronización de faltantes completada (modo stop por IDs conocidos).")
        _add_report(
            "paso4_sync_faltantes",
            "ok",
            {"max_chats": args.max_chats, "metodo": "sync_con_stop"},
        )
    log_timing("Terminando paso 4")

    log_timing("\n=== Paso 5: Indexar histórico ===")
    log_timing("Iniciando paso 5")
    if args.skip_historico:
        log_timing("Saltado por --skip-historico.")
        _add_report("paso5_indexar_historico", "skipped", {"reason": "skip_historico"})
    elif args.dry_run:
        log_timing("Saltado (dry-run).")
        _add_report("paso5_indexar_historico", "skipped", {"reason": "dry_run"})
    else:
        await indexar_historico_async(args.max_chats)
        _add_report("paso5_indexar_historico", "ok", {"max_chats": args.max_chats})
    log_timing("Terminando paso 5")

    log_timing("\n=== Paso 6: Recalcular indexados (post) ===")
    log_timing("Iniciando paso 6")
    await recalcular_indexados_async()
    _add_report("paso6_recalcular_indexados_post", "ok")
    log_timing("Terminando paso 6")

    log_timing("\n=== Paso 7: Recalcular duplicados (post) ===")
    log_timing("Iniciando paso 7")
    await contar_duplicados_y_actualizar()
    _add_report("paso7_duplicados_post", "ok")
    log_timing("Terminando paso 7")

    log_timing("\n✅ Pipeline completado.")
    _add_report("pipeline", "completed", {"report_path": REPORT_PATH})
    _write_report()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline programado para conteo, sincronización y limpieza básica de métricas de videos."
    )
    parser.add_argument(
        "--max-nuevos",
        type=int,
        default=30,
        help="Número de videos consecutivos existentes para detener la sincronización por canal.",
    )
    parser.add_argument(
        "--max-chats",
        type=int,
        default=None,
        help="Máximo de chats a considerar en fases que recorren chats (faltantes e histórico).",
    )
    parser.add_argument(
        "--skip-historico",
        action="store_true",
        help="Omitir la fase de indexado histórico.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No escribe en BD ni llama a Telegram (omite pasos de red).",
    )
    parser.add_argument(
        "--guardar-chats",
        action="store_true",
        help="Ejecutar paso 0 (guardar/actualizar chats). Por defecto se omite para velocidad.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
