import argparse
import asyncio
import os
import sys
import datetime
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

# Reuso de scripts existentes
from CLI.cantidad_videos.auditar_conteo_videos_chats import (  # noqa: E402
    contar_videos_en_todos_mis_chats,
)
from CLI.cantidad_videos.recalcular_indexados_desde_bd import (  # noqa: E402
    main as recalcular_indexados_main,
)
from CLI.cantidad_videos.indexador_historico import (  # noqa: E402
    obtener_chats_con_historial,
    escanear_historia_antigua,
)
from CLI.cantidad_videos.sincronizar_faltantes_search import (  # noqa: E402
    sync_faltantes_por_busqueda,
)


# ---------------------------------------------------------------------------
# Helpers SQL
# ---------------------------------------------------------------------------
async def contar_duplicados_y_actualizar() -> None:
    """
    Cuenta duplicados por chat (file_unique_id con count>1) y actualiza chat_video_counts. 
    No borra ni marca, solo escribe el número de duplicados.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Mapa chat_id -> duplicados
        query = """
            WITH dupes AS (
                SELECT chat_id, COUNT(*) AS c
                FROM (
                    SELECT chat_id, file_unique_id, COUNT(*) AS n
                    FROM videos_telegram
                    GROUP BY chat_id, file_unique_id
                    HAVING n > 1
                )
                GROUP BY chat_id
            )
            SELECT chat_id, c FROM dupes
        """
        dupes: dict[int, int] = {}
        async with db.execute(query) as cur:
            async for row in cur:
                dupes[int(row[0])] = int(row[1])

        # Actualizar tabla (si no hay duplicados, poner 0)
        async with db.execute("SELECT chat_id FROM chat_video_counts") as cur:
            updates = []
            async for row in cur:
                chat_id = int(row[0])
                updates.append((dupes.get(chat_id, 0), chat_id))

        await db.executemany(
            "UPDATE chat_video_counts SET duplicados = ? WHERE chat_id = ?", updates
        )
        await db.commit()


async def recalcular_indexados_async() -> None:
    """
    Llama al recalculador síncrono en un hilo para poder usar await.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, recalcular_indexados_main)


async def contar_videos_nube_async() -> None:
    await contar_videos_en_todos_mis_chats()


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
                print(f"[WARN] Error escaneando histórico {chat_id}: {e}")
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

    print("\n=== Paso 1: Contar videos en la nube ===")
    if args.dry_run:
        print("Saltado (dry-run).")
    else:
        await contar_videos_nube_async()

    print("\n=== Paso 2: Recalcular indexados desde BD ===")
    await recalcular_indexados_async()

    print("\n=== Paso 3: Calcular duplicados ===")
    await contar_duplicados_y_actualizar()

    print(f"\n=== Paso 4: Sincronizar faltantes (detener tras {args.max_nuevos} consecutivos existentes) ===")
    if args.dry_run:
        print("Saltado (dry-run).")
    else:
        await sync_faltantes_por_busqueda(
            max_chats=args.max_chats,
            only_chat_id=None,
            consecutivos_para_detener=args.max_nuevos,
        )
        print("Sincronización de faltantes completada.")

    print("\n=== Paso 5: Indexar histórico ===")
    if args.skip_historico:
        print("Saltado por --skip-historico.")
    elif args.dry_run:
        print("Saltado (dry-run).")
    else:
        await indexar_historico_async(args.max_chats)

    print("\n=== Paso 6: Recalcular indexados (post) ===")
    await recalcular_indexados_async()

    print("\n=== Paso 7: Recalcular duplicados (post) ===")
    await contar_duplicados_y_actualizar()

    print("\n✅ Pipeline completado.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline programado para conteo, sincronización y limpieza básica de métricas de videos."
    )
    parser.add_argument(
        "--max-nuevos",
        type=int,
        default=5,
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
