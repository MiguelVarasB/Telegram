"""
Operaciones de base de datos relacionadas con carpetas.
Versión Async con aiosqlite.
"""
import datetime
import aiosqlite
from typing import List
from config import DB_PATH
from .chats import db_get_chat_folders


async def get_folder_items_from_db(folder_id: int, folder_name: str | None = None) -> List[dict]:
    """
    Devuelve la lista de chats de una carpeta desde la BD local (Async).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Consulta optimizada
        query = """
            SELECT
                c.chat_id,
                c.name,
                c.type,
                c.photo_id,
                c.username,
                c.last_message_date,
                COUNT(v.id) AS video_count
            FROM chat_folders cf
            JOIN chats c ON c.chat_id = cf.chat_id
            LEFT JOIN videos_telegram v ON v.chat_id = c.chat_id
            WHERE cf.folder_id = ?
            GROUP BY c.chat_id, c.name, c.type, c.photo_id, c.username, c.last_message_date
            ORDER BY c.last_message_date IS NULL,
                     c.last_message_date DESC,
                     c.name COLLATE NOCASE
        """
        async with db.execute(query, (folder_id,)) as cursor:
            rows = await cursor.fetchall()

    items: List[dict] = []
    
    # Procesar resultados sin bloquear I/O
    for row in rows:
        chat_id, name, chat_type, photo_id, username, last_msg_dt, video_count = row
        
        # Ahora usamos await porque db_get_chat_folders es async
        folders = await db_get_chat_folders(chat_id)
        
        display_name = name or f"Chat {chat_id}"
        info_text = chat_type or "cache"
        
        if last_msg_dt:
            try:
                dt = datetime.datetime.fromisoformat(last_msg_dt)
                info_text = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                info_text = last_msg_dt
        
        if video_count is not None:
            info_text = f"{video_count} videos · {info_text}"

        telegram_link = None
        if username:
            telegram_link = f"https://t.me/{username}"
        
        link = f"/channel/{chat_id}?name={display_name}"
        if folder_name:
            link += f"&folder_id={folder_id}&folder_name={folder_name}"

        items.append({
            "name": display_name,
            "count": info_text,
            "link": link,
            "type": "chat",
            "photo_id": photo_id,
            "folder_id": folder_id,
            "folders": folders,
            "chat_id": chat_id,
            "username": username,
            "telegram_link": telegram_link,
        })
    
    return items