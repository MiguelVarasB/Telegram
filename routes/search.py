"""
Rutas de búsqueda (local y global).
"""
from typing import Literal, Optional

from fastapi import APIRouter, Query
import aiosqlite
from pyrogram import enums

from config import DB_PATH
from services import get_client

router = APIRouter()

# Búsqueda principal; acepta alias /api/search para compatibilidad con el front.
@router.get("/search")
async def api_search(
    q: str = Query(..., min_length=1),
    scope: Literal["local", "global"] = "local",
    type: Literal["video", "enlace", "chat", "archivo"] = "video",   

    # Límite de resultados: por defecto 20, mínimo 1, máximo 100 (validado por FastAPI).
    limit: int = Query(20, ge=1, le=100),
    chat_id: Optional[int] = Query(None),
):
    return await _do_search(q, scope, type, limit, chat_id)


@router.get("/api/search")
async def api_search_alias(
    q: str = Query(..., min_length=1),
    scope: Literal["local", "global"] = "local",
    type: Literal["video", "enlace", "chat", "archivo"] = "video",
    limit: int = Query(20, ge=1, le=100),
    chat_id: Optional[int] = Query(None),
):
    # Alias para evitar 404 si el front apunta a /api/search
    return await _do_search(q, scope, type, limit, chat_id)


async def _do_search(
    q: str,
    scope: Literal["local", "global"],
    type: Literal["video", "enlace", "chat", "archivo"],
    limit: int,
    chat_id: Optional[int],
):
    """
    Endpoint de búsqueda.
    - scope: local (BD) o global (Telegram).
    - type:
        - video: videos
        - enlace: mensajes con URLs
        - chat: chats/canales
        - archivo: documentos (global) / vacío local
    Retorna JSON con los resultados.
    """
    q_like = f"%{q}%"

    if scope == "local":
        if type == "video":
            # Consulta en la base local; el límite se impone en SQL.
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                base_sql = """
                    SELECT id, chat_id, message_id, nombre, caption, fecha_mensaje,
                           duracion, tamano_bytes, mime_type, es_vertical
                    FROM videos_telegram
                    WHERE (nombre LIKE ? OR caption LIKE ?)
                """
                params = [q_like, q_like]
                if chat_id is not None:
                    base_sql += " AND chat_id = ?"
                    params.append(chat_id)

                base_sql += """
                    ORDER BY fecha_mensaje DESC
                    LIMIT ?
                """
                params.append(limit)

                async with db.execute(base_sql, params) as cursor:
                    rows = await cursor.fetchall()
            items = [dict(r) for r in rows]
            return {"scope": scope, "type": type, "items": items}

        # Local enlaces / chat / archivo: sin datos en BD actual
        return {"scope": scope, "type": type, "items": []}

    # scope == "global"
    client = get_client()

    if type == "chat":
        # Buscar en diálogos (nombres/usernames) y cortar tras 'limit'
        dialogs = await client.get_dialogs(limit=200)
        items = []
        for d in dialogs:
            title = d.chat.title or getattr(d.chat, "first_name", "") or ""
            username = getattr(d.chat, "username", None)
            if q.lower() in title.lower() or (username and q.lower() in username.lower()):
                items.append(
                    {
                        "chat_id": d.chat.id,
                        "name": title,
                        "username": username,
                        "type": str(d.chat.type),
                    }
                )
            if len(items) >= limit:
                break
        return {"scope": scope, "type": type, "items": items}

    # Mapear filtros de Telegram (Pyrogram search_messages)
    filter_map = {
        "video": enums.MessagesFilter.VIDEO,
        "enlace": enums.MessagesFilter.URL,
        "archivo": enums.MessagesFilter.DOCUMENT,
    }
    msg_filter = filter_map.get(type, enums.MessagesFilter.EMPTY)

    # Algunas versiones de Pyrogram no tienen search_global_messages.
    # Usamos search_messages con chat_id="me" como pseudo-global.
    items = []
    async for m in client.search_messages(
        chat_id="me",
        query=q,
        filter=msg_filter,
        limit=limit,
    ):
        # Adaptar el mensaje a un dict simple para el front.
        items.append(
            {
                "chat_id": m.chat.id if m.chat else None,
                "message_id": m.id,
                "date": m.date.isoformat() if m.date else None,
                "text": m.text or m.caption,
                "views": m.views,
                "forwards": m.forwards,
                "media": str(m.media) if m.media else None,
                "mime_type": getattr(m.document, "mime_type", None) if m.document else None,
                "file_name": getattr(m.document, "file_name", None) if m.document else None,
                "duration": getattr(m.video, "duration", None) if m.video else None,
                "file_size": getattr(m.document, "file_size", None)
                or getattr(m.video, "file_size", None),
            }
        )

    return {"scope": scope, "type": type, "items": items}
