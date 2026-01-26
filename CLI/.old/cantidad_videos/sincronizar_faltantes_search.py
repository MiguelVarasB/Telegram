import argparse
import asyncio
import json
import os
import sys
from collections import deque
from pathlib import Path
from typing import Iterable, Tuple

import aiosqlite
from pyrogram import enums
from pyrogram.errors import FloodWait

"""Resumen: Sincroniza videos faltantes por canal usando search_messages (VIDEO) hasta un máximo configurado."""
## NO USAR EN EL PIPELINE###
# Asegurar import desde la raíz del proyecto (Telegram)
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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
        SELECT cvc.chat_id,
               cvc.videos_count,
               cvc.indexados,
               COALESCE(cvc.duplicados, 0) AS duplicados,
               (cvc.videos_count - COALESCE(cvc.duplicados, 0) - COALESCE(cvc.indexados, 0)) AS faltantes
        FROM chat_video_counts cvc
        JOIN chats c ON c.chat_id = cvc.chat_id
        WHERE cvc.videos_count IS NOT NULL AND cvc.videos_count > 0
          AND c.activo = 1
          AND COALESCE(c.is_owner, 0) = 0
          AND (cvc.videos_count - COALESCE(cvc.duplicados, 0) - COALESCE(cvc.indexados, 0)) > 0
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


async def existe_video_en_bd(file_unique_id: str) -> bool:
    """
    Verifica si un video ya existe en la base de datos.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM videos_telegram WHERE file_unique_id = ? LIMIT 1",
            (file_unique_id,),
        ) as cur:
            row = await cur.fetchone()
            return row is not None


async def sync_faltantes_por_busqueda(
    max_videos_por_chat: int = 200,
    max_chats: int | None = None,
    only_chat_id: int | None = None,
    consecutivos_para_detener: int = 5,
):
    """
    Sincroniza videos faltantes por canal usando search_messages (filtro VIDEO).
    Se detiene cuando encuentra 'consecutivos_para_detener' videos que ya existen en la BD.
    No hay límite de videos nuevos a indexar.
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
                f"Faltan {faltantes}. Sincronizando hasta encontrar {consecutivos_para_detener} consecutivos existentes."
            )

            count_new = 0
            count_existing = 0
            consecutivos_existentes = 0
            seen_files: set[str] = set()

            batch_size = 100
            offset = 0

            detener_chat = False

            while True:
                batch: deque = deque()
                try:
                    async for m in client.search_messages(
                        chat_id=chat_id,
                        query="",
                        filter=enums.MessagesFilter.VIDEO,
                        offset=offset,
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
                        offset += 1
                        continue

                    v = m.video
                    if v.file_unique_id in seen_files:
                        offset += 1
                        continue
                    seen_files.add(v.file_unique_id)

                    # Verificar si ya existe en BD
                    ya_existe = await existe_video_en_bd(v.file_unique_id)

                    if ya_existe:
                        consecutivos_existentes += 1
                        count_existing += 1
                        fn = v.file_name or f"Video {m.id}"
                        print(
                            f"  ℹ️  Ya existe: {fn[:40]} | msg_id={m.id} | chat_id={chat_id} "
                            f"(consecutivos: {consecutivos_existentes}/{consecutivos_para_detener})"
                        )
                        # Asegurar que el mensaje quede registrado aunque el video ya exista
                        msg_dict = json.loads(str(m))
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
                            "caption": m.caption,
                        }

                        await db_upsert_video_message(message_data)

                        # Chequear umbral inmediatamente para no seguir dentro del batch
                        if consecutivos_existentes >= consecutivos_para_detener:
                            print(f"  🛑 Deteniendo: {consecutivos_para_detener} videos consecutivos ya existentes")
                            detener_chat = True
                            break

                    else:
                        # Video nuevo - resetear contador de consecutivos
                        consecutivos_existentes = 0

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

                    # Avanzar offset siempre, para paginar correctamente
                    offset += 1

                    # Log solo para los nuevos
                    if not ya_existe:
                        print(f"  ✅ Nuevo: {fn[:40]} (total nuevos: {count_new})")

                # Salir si se alcanzó el límite de consecutivos dentro del batch
                if detener_chat:
                    break

                if len(batch) < batch_size:
                    print(f"  ℹ️  No hay más videos en el canal")
                    break

            print(f"✅ Canal {chat_id}: {count_new} videos nuevos indexados, {count_existing} ya existían.")

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
