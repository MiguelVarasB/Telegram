import argparse
import asyncio
import json
import os
import sys
from typing import Iterable, Tuple

import aiosqlite
from pyrogram import enums
from pyrogram.errors import FloodWait

"""Resumen: Indexa primeros videos faltantes por canal usando get_chat_history y actualiza tablas principales."""


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH  # noqa: E402
from database import (  # noqa: E402
    db_count_videos_by_chat,
    db_upsert_chat_video_count,
)
from services.telegram_client import get_client  # noqa: E402
from services.video_processor import procesar_mensaje_video  # noqa: E402


async def get_chats_incompletos(max_chats: int | None = None) -> Iterable[Tuple[int, int, int | None, int, int]]:
    """
    Devuelve (chat_id, videos_count, indexados, duplicados) de los canales/supergrupos
    donde indexados < videos_count.
    """
    query = """
        SELECT chat_id,
               videos_count,
               indexados,
               COALESCE(duplicados, 0) AS duplicados,
               (videos_count - COALESCE(duplicados, 0) - COALESCE(indexados, 0)) AS faltantes
        FROM chat_video_counts
        WHERE videos_count IS NOT NULL AND videos_count > 0
          AND (videos_count - COALESCE(duplicados, 0) - COALESCE(indexados, 0)) > 0
        ORDER BY faltantes DESC
    """
    params: tuple = ()
    if max_chats is not None:
        query += " LIMIT ?"
        params = (max_chats,)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
    return rows


async def indexar_primeros_videos(
    max_videos_por_chat: int = 100,
    max_chats: int | None = None,
    only_chat_id: int | None = None,
):
    """
    Indexa hasta `max_videos_por_chat` videos de cada canal con indexado incompleto.
    Usa search_messages (videos) y hace upsert en las tablas de la app principal.
    """
    client = get_client(clone_for_cli=True)

    await client.start()
    print("✅ Sesión Pyrogram iniciada (usuario).")

    try:
        chats = await get_chats_incompletos(max_chats=max_chats)
        if only_chat_id is not None:
            chats = [row for row in chats if row[0] == only_chat_id]

        if not chats:
            print("No hay canales con indexación incompleta.")
            return

        print(f"Se procesarán {len(chats)} canales incompletos.")

        for chat_id, total_videos, indexados, duplicados, faltantes in chats:
            total_unicos = max(total_videos - duplicados, 0)
            print(
                f"\n📺 Canal {chat_id}: {indexados or 0} indexados / "
                f"{total_unicos} únicos ({total_videos} totales, {duplicados} dupes). "
                f"Faltan {faltantes}."
            )

            count_new = 0
            seen_files: set[str] = set()

            try:
                from collections import deque

                buffer_msgs = deque(maxlen=max_videos_por_chat)
                async for m in client.get_chat_history(chat_id=chat_id):
                    if not m.video:
                        continue
                    # guardamos siempre los más antiguos (cola) quedándonos con los últimos max_videos_por_chat del recorrido completo
                    buffer_msgs.appendleft(m)

                for m in buffer_msgs:  # ya están de antiguo a nuevo
                    v = m.video
                    if v.file_unique_id in seen_files:
                        continue
                    seen_files.add(v.file_unique_id)

                    resultado = await procesar_mensaje_video(m, origen="first100")
                    
                    if resultado["procesado"]:
                        count_new += 1
                    if count_new % 25 == 0:
                        print(f"  · {count_new} videos procesados en {chat_id}...")

            except FloodWait as e:
                print(f"⏳ FloodWait de {e.value}s en {chat_id}, esperando...")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"⚠️ Error escaneando {chat_id}: {e}")

            print(f"✅ Canal {chat_id}: {count_new} videos guardados/actualizados.")

    finally:
        await client.stop()
        print("🛑 Cliente de Telegram detenido")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Indexa los primeros videos (por defecto 100) de canales que no estén completamente indexados."
    )
    parser.add_argument(
        "--limit-per-chat",
        type=int,
        default=100,
        help="Cantidad máxima de videos a traer por canal incompleto (default: 100).",
    )
    parser.add_argument(
        "--max-chats",
        type=int,
        default=None,
        help="Número máximo de canales a procesar (default: todos los incompletos).",
    )
    parser.add_argument(
        "--only-chat-id",
        type=int,
        default=None,
        help="Procesa solo este chat_id (útil para pruebas puntuales).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(
        indexar_primeros_videos(
            max_videos_por_chat=args.limit_per_chat,
            max_chats=args.max_chats,
            only_chat_id=args.only_chat_id,
        )
    )

#