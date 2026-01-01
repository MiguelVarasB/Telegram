import argparse
import asyncio
import json
import os
import sys
from collections import deque
from typing import Iterable, Tuple

import aiosqlite
from pyrogram import enums
from pyrogram.errors import FloodWait

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH  # noqa: E402
from database import (  # noqa: E402
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
)
from services.telegram_client import get_client  # noqa: E402


async def get_chats_incompletos(max_chats: int | None = None) -> Iterable[Tuple[int, int, int | None, int, int]]:
    """
    Devuelve (chat_id, videos_count, indexados, duplicados, faltantes) de los canales/supergrupos
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


async def sync_faltantes_por_busqueda(
    max_videos_por_chat: int = 200,
    max_chats: int | None = None,
    only_chat_id: int | None = None,
):
    """
    Sincroniza videos faltantes por canal usando search_messages (filtro VIDEO).
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
            objetivo = min(faltantes, max_videos_por_chat)
            print(
                f"\n📺 Canal {chat_id}: {indexados or 0} indexados / "
                f"{total_unicos} únicos ({total_videos} totales, {duplicados} dupes). "
                f"Faltan {faltantes} -> se intentan {objetivo}."
            )

            count_new = 0
            seen_files: set[str] = set()

            batch_size = 100

            while count_new < objetivo:
                batch: deque = deque()
                try:
                    async for m in client.search_messages(
                        chat_id=chat_id,
                        query="",
                        filter=enums.MessagesFilter.VIDEO,
                        offset=count_new,
                        limit=batch_size,
                    ):
                        batch.append(m)
                except FloodWait as e:
                    print(f"⏳ FloodWait de {e.value}s en {chat_id}, esperando...")
                    await asyncio.sleep(e.value)
                    continue
                except Exception as e:
                    print(f"⚠️ Error buscando en {chat_id}: {e}")
                    break

                if not batch:
                    break

                for m in batch:
                    if not m.video:
                        continue

                    v = m.video
                    if v.file_unique_id in seen_files:
                        continue
                    seen_files.add(v.file_unique_id)

                    fn = v.file_name or f"Video {m.id}"
                    msg_dict = json.loads(str(m))

                    video_data = {
                        "chat_id": chat_id,
                        "message_id": m.id,
                        "file_id": v.file_id,
                        "file_unique_id": v.file_unique_id,
                        "nombre": fn,
                        "caption": m.caption,
                        "tamano_bytes": v.file_size,
                        "fecha_mensaje": m.date.isoformat() if m.date else None,
                        "duracion": v.duration or 0,
                        "ancho": v.width or 0,
                        "alto": v.height or 0,
                        "mime_type": v.mime_type,
                        "views": m.views or 0,
                        "outgoing": m.outgoing,
                    }
                    await db_upsert_video(video_data)
                    await db_add_video_file_id(v.file_unique_id, v.file_id, v.file_unique_id, "search_sync")

                    msg_from = getattr(m, "from_user", None)
                    fwd_chat = msg_dict.get("forward_from_chat") or {}
                    message_data = {
                        "video_id": v.file_unique_id,
                        "chat_id": chat_id,
                        "message_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "from_user": {
                            "id": getattr(msg_from, "id", None),
                            "username": getattr(msg_from, "username", None),
                            "is_bot": getattr(msg_from, "is_bot", False),
                        }
                        if msg_from
                        else {},
                        "media": msg_dict.get("media"),
                        "views": m.views or 0,
                        "forwards": getattr(m, "forwards", None),
                        "outgoing": int(m.outgoing) if m.outgoing is not None else None,
                        "reply_to_message_id": getattr(m, "reply_to_message_id", None),
                        "forward_from_chat": fwd_chat,
                        "forward_from_message_id": msg_dict.get("forward_from_message_id"),
                        "forward_date": msg_dict.get("forward_date"),
                        "caption": msg_dict.get("caption"),
                    }

                    await db_upsert_video_message(message_data)

                    count_new += 1
                    if count_new % 25 == 0:
                        print(f"  · {count_new} videos procesados en {chat_id}...")
                    if count_new >= objetivo:
                        break

                if len(batch) < batch_size:
                    break

            print(f"✅ Canal {chat_id}: {count_new} videos guardados/actualizados (límite {objetivo}).")

    finally:
        await client.stop()
        print("🛑 Cliente de Telegram detenido")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza videos faltantes por canal usando search_messages (filtro VIDEO)."
    )
    parser.add_argument(
        "--max-chats",
        type=int,
        default=None,
        help="Número máximo de canales a procesar (por defecto todos los incompletos).",
    )
    parser.add_argument(
        "--max-por-chat",
        type=int,
        default=20000,
        help="Número máximo de videos a intentar sincronizar por canal (por defecto 200).",
    )
    parser.add_argument(
        "--only-chat-id",
        type=int,
        default=None,
        help="Si se indica, solo procesa ese chat_id concreto.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        sync_faltantes_por_busqueda(
            max_videos_por_chat=args.max_por_chat,
            max_chats=args.max_chats,
            only_chat_id=args.only_chat_id,
        )
    )
