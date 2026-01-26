import asyncio
import datetime
import os
import sys
from pathlib import Path

from pyrogram import enums
from pyrogram.errors import FloodWait

# Asegurar imports desde la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from services.telegram_client import get_client  # noqa: E402
from database import (  # noqa: E402
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
)
from utils import log_timing  # noqa: E402
DIAS_ATRAS=1
LIMIT=1000

def _mensaje_to_dict(m, file_unique_id: str) -> dict:
    msg_from = getattr(m, "from_user", None)
    fwd_chat = getattr(m, "forward_from_chat", None)
    return {
        "video_id": file_unique_id,
        "chat_id": getattr(m.chat, "id", None),
        "message_id": m.id,
        "date": m.date.isoformat() if getattr(m, "date", None) else None,
        "from_user": {
            "id": getattr(msg_from, "id", None),
            "username": getattr(msg_from, "username", None),
            "is_bot": getattr(msg_from, "is_bot", False),
        }
        if msg_from
        else {},
        "media": str(getattr(m, "media", None)) if getattr(m, "media", None) else None,
        "views": getattr(m, "views", 0) or 0,
        "forwards": getattr(m, "forwards", None),
        "outgoing": int(m.outgoing) if getattr(m, "outgoing", None) is not None else None,
        "reply_to_message_id": getattr(m, "reply_to_message_id", None),
        "forward_from_chat": {
            "id": getattr(fwd_chat, "id", None),
            "username": getattr(fwd_chat, "username", None),
            "title": getattr(fwd_chat, "title", None),
        }
        if fwd_chat
        else {},
        "forward_from_message_id": getattr(m, "forward_from_message_id", None),
        "forward_date": getattr(m, "forward_date", None),
        "caption": getattr(m, "caption", None),
    }


def _video_to_dict(m, file_unique_id: str) -> dict:
    v = m.video or m.document
    return {
        "chat_id": getattr(m.chat, "id", None),
        "message_id": m.id,
        "file_id": v.file_id,
        "file_unique_id": file_unique_id,
        "nombre": getattr(v, "file_name", None) or getattr(m, "caption", None) or "sin_nombre",
        "caption": getattr(m, "caption", None),
        "tamano_bytes": getattr(v, "file_size", 0) or 0,
        "fecha_mensaje": m.date.isoformat() if getattr(m, "date", None) else None,
        "duracion": getattr(v, "duration", 0) or 0,
        "ancho": getattr(v, "width", 0) or 0,
        "alto": getattr(v, "height", 0) or 0,
        "mime_type": getattr(v, "mime_type", None),
        "views": getattr(m, "views", 0) or 0,
        "outgoing": getattr(m, "outgoing", None),
    }


async def _iterar_global_videos(client, limit: int):
    """Generador que usa search_global si existe, o fallback a search_messages."""
    try:
        async for m in client.search_global(
            query="",
            filter=enums.MessagesFilter.VIDEO,
            limit=limit,
        ):
            yield m
    except AttributeError:
        # Fallback: usar chat "me" como pseudo-global
        async for m in client.search_messages(
            chat_id="me",
            query="",
            filter=enums.MessagesFilter.VIDEO,
            limit=limit,
        ):
            yield m
    except FloodWait as e:
        log_timing(f"⏳ FloodWait global {e.value}s, esperando...")
        await asyncio.sleep(e.value)


async def obtener_videos_recientes_global(dias: int = DIAS_ATRAS, limit: int = LIMIT):
    """Obtiene videos recientes (últimos `dias`) vía búsqueda global y los guarda en BD."""
    client = get_client(clone_for_cli=True)
    started_here = False
    if not client.is_connected:
        await client.start()
        started_here = True

    corte = datetime.datetime.utcnow() - datetime.timedelta(days=dias)
    log_timing(f"🔍 Buscando videos globales desde {corte.isoformat()} (limit={limit})")

    nuevos = 0
    existentes = 0
    procesados = 0

    try:
        async for m in _iterar_global_videos(client, limit=limit):
            if m.date and m.date.replace(tzinfo=None) < corte:
                log_timing("⏹️ Alcanzada fecha de corte, deteniendo búsqueda.")
                break

            procesados += 1
            v = m.video or m.document
            if v is None:
                continue
            file_unique_id = v.file_unique_id

            video_data = _video_to_dict(m, file_unique_id)
            log_timing(f"🔍 analizando {file_unique_id})")
            await db_upsert_video(video_data)
            await db_add_video_file_id(file_unique_id, v.file_id, file_unique_id, "search_global")

            message_data = _mensaje_to_dict(m, file_unique_id)
            await db_upsert_video_message(message_data)

            nuevos += 1

    finally:
        if started_here and client.is_connected:
            await client.stop()

    log_timing(f"✅ Finalizado. Procesados: {procesados}, nuevos guardados (upsert): {nuevos}, exist/actualizados: {existentes}")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Obtiene videos recientes vía search_global (últimos días).")
    parser.add_argument("--dias", type=int, default=DIAS_ATRAS, help="Días hacia atrás (UTC) para cortar la búsqueda.")
    parser.add_argument("--limit", type=int, default=LIMIT, help="Límite bruto de resultados a revisar.")
    args = parser.parse_args()

    await obtener_videos_recientes_global(dias=args.dias, limit=args.limit)


if __name__ == "__main__":
    asyncio.run(main())
