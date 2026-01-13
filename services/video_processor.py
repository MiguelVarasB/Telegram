import json
from typing import Optional

import aiosqlite

from config import DB_PATH
from database.videos import (
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
    db_bulk_upsert_videos,
    db_bulk_upsert_video_messages,
    db_bulk_add_video_file_ids,
)


async def existe_video_en_bd(file_unique_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM videos_telegram WHERE file_unique_id = ? LIMIT 1",
            (file_unique_id,),
        ) as cur:
            row = await cur.fetchone()
            return row is not None


async def existe_mensaje_en_bd(chat_id: int, message_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM video_messages WHERE chat_id = ? AND message_id = ? LIMIT 1",
            (chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
            return row is not None


async def procesar_mensajes_video_batch(messages: list, origen: str = "generic") -> list[dict]:
    """
    Procesa múltiples mensajes de video en batch para reducir overhead de DB.
    Retorna lista de resultados por mensaje.
    """
    if not messages:
        return []

    resultados = []
    async with aiosqlite.connect(DB_PATH) as db:
        # Pre-cargar existencia de videos y mensajes en una sola query
        unique_ids = [m.video.file_unique_id for m in messages if m.video]
        chat_msg_pairs = [(m.chat.id, m.id) for m in messages if m.video]
        
        videos_existentes = set()
        if unique_ids:
            placeholders = ','.join('?' * len(unique_ids))
            async with db.execute(
                f"SELECT file_unique_id FROM videos_telegram WHERE file_unique_id IN ({placeholders})",
                unique_ids
            ) as cur:
                async for row in cur:
                    videos_existentes.add(row[0])
        
        mensajes_existentes = set()
        if chat_msg_pairs:
            # Construir query con múltiples pares (chat_id, message_id)
            conditions = ' OR '.join(['(chat_id = ? AND message_id = ?)'] * len(chat_msg_pairs))
            params = []
            for chat_id, msg_id in chat_msg_pairs:
                params.extend([chat_id, msg_id])
            async with db.execute(
                f"SELECT chat_id, message_id FROM video_messages WHERE {conditions}",
                params
            ) as cur:
                async for row in cur:
                    mensajes_existentes.add((row[0], row[1]))

        # Preparar datos para bulk insert
        videos_batch = []
        messages_batch = []
        file_ids_batch = []
        
        for message in messages:
            if not message.video:
                resultados.append({"procesado": False, "razon": "sin_video"})
                continue

            video = message.video
            unique_id = video.file_unique_id
            video_existia = unique_id in videos_existentes
            mensaje_existia = (message.chat.id, message.id) in mensajes_existentes

            video_data = {
                "file_unique_id": unique_id,
                "chat_id": message.chat.id,
                "message_id": message.id,
                "file_id": video.file_id,
                "nombre": video.file_name or f"vid_{message.id}.mp4",
                "caption": message.caption,
                "tamano_bytes": video.file_size,
                "fecha_mensaje": message.date.isoformat() if message.date else None,
                "duracion": video.duration or 0,
                "ancho": video.width or 0,
                "alto": video.height or 0,
                "mime_type": video.mime_type,
                "views": message.views or 0,
                "outgoing": message.outgoing,
            }
            videos_batch.append(video_data)

            from_user = getattr(message, "from_user", None)
            forward_from_chat = getattr(message, "forward_from_chat", None)

            message_data = {
                "video_id": unique_id,
                "chat_id": message.chat.id,
                "message_id": message.id,
                "date": message.date.isoformat() if message.date else None,
                "from_user": {
                    "id": getattr(from_user, "id", None),
                    "username": getattr(from_user, "username", None),
                    "is_bot": getattr(from_user, "is_bot", None),
                } if from_user else {},
                "media": "video",
                "views": message.views or 0,
                "forwards": message.forwards or 0,
                "outgoing": message.outgoing,
                "reply_to_message_id": getattr(message, "reply_to_message_id", None),
                "forward_from_chat": {
                    "id": getattr(forward_from_chat, "id", None),
                    "title": getattr(forward_from_chat, "title", None),
                } if forward_from_chat else {},
                "forward_from_message_id": getattr(message, "forward_from_message_id", None),
                "forward_date": (
                    message.forward_date.isoformat() if getattr(message, "forward_date", None) else None
                ),
                "caption": message.caption,
            }
            messages_batch.append(message_data)
            
            file_ids_batch.append((unique_id, video.file_id, unique_id, origen))

            resultados.append({
                "procesado": True,
                "video_nuevo": not video_existia,
                "mensaje_nuevo": not mensaje_existia,
                "file_unique_id": unique_id,
                "chat_id": message.chat.id,
                "message_id": message.id,
            })

    # Ejecutar bulk inserts fuera del context manager de DB (usan su propia conexión)
    try:
        if videos_batch:
            await db_bulk_upsert_videos(videos_batch)
        if messages_batch:
            await db_bulk_upsert_video_messages(messages_batch)
        if file_ids_batch:
            await db_bulk_add_video_file_ids(file_ids_batch)
    except Exception as e:
        # Si falla el bulk, marcar todos como error
        for i in range(len(resultados)):
            if resultados[i].get("procesado"):
                resultados[i] = {
                    "procesado": False,
                    "razon": "error_guardado_bulk",
                    "error": str(e),
                    "chat_id": resultados[i]["chat_id"],
                    "message_id": resultados[i]["message_id"],
                }

    return resultados


async def procesar_mensaje_video(message, origen: str = "generic", incluir_raw_json: bool = False) -> dict:
    if not message.video:
        return {"procesado": False, "razon": "sin_video"}

    video = message.video
    unique_id = video.file_unique_id

    video_existia = await existe_video_en_bd(unique_id)
    mensaje_existia = await existe_mensaje_en_bd(message.chat.id, message.id)

    video_data = {
        "file_unique_id": unique_id,
        "chat_id": message.chat.id,
        "message_id": message.id,
        "file_id": video.file_id,
        "nombre": video.file_name or f"vid_{message.id}.mp4",
        "caption": message.caption,
        "tamano_bytes": video.file_size,
        "fecha_mensaje": message.date.isoformat() if message.date else None,
        "duracion": video.duration or 0,
        "ancho": video.width or 0,
        "alto": video.height or 0,
        "mime_type": video.mime_type,
        "views": message.views or 0,
        "outgoing": message.outgoing,
    }

    try:
        await db_upsert_video(video_data)
        await db_add_video_file_id(unique_id, video.file_id, unique_id, origen=origen)

        from_user = getattr(message, "from_user", None)
        forward_from_chat = getattr(message, "forward_from_chat", None)

        message_data = {
            "video_id": unique_id,
            "chat_id": message.chat.id,
            "message_id": message.id,
            "date": message.date.isoformat() if message.date else None,
            "from_user": {
                "id": getattr(from_user, "id", None),
                "username": getattr(from_user, "username", None),
                "is_bot": getattr(from_user, "is_bot", None),
            }
            if from_user
            else {},
            "media": "video",
            "views": message.views or 0,
            "forwards": message.forwards or 0,
            "outgoing": message.outgoing,
            "reply_to_message_id": getattr(message, "reply_to_message_id", None),
            "forward_from_chat": {
                "id": getattr(forward_from_chat, "id", None),
                "title": getattr(forward_from_chat, "title", None),
            }
            if forward_from_chat
            else {},
            "forward_from_message_id": getattr(message, "forward_from_message_id", None),
            "forward_date": (
                message.forward_date.isoformat() if getattr(message, "forward_date", None) else None
            ),
            "caption": message.caption,
        }

        if incluir_raw_json:
            try:
                msg_dict = json.loads(str(message))
                message_data["raw_json"] = msg_dict
            except Exception:
                pass

        await db_upsert_video_message(message_data)

        return {
            "procesado": True,
            "video_nuevo": not video_existia,
            "mensaje_nuevo": not mensaje_existia,
            "file_unique_id": unique_id,
            "chat_id": message.chat.id,
            "message_id": message.id,
        }

    except Exception as e:
        return {
            "procesado": False,
            "razon": "error_guardado",
            "error": str(e),
            "chat_id": message.chat.id,
            "message_id": message.id,
        }
