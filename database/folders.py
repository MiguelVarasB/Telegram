"""
Operaciones de base de datos relacionadas con carpetas.
Versión Async con aiosqlite.
"""
import datetime
import aiosqlite
from typing import List
from config import DB_PATH, CACHE_DUMP_VIDEOS_CHANNEL_ID
from .chats import db_get_chat_folders
from utils import serialize_pyrogram, json_serial, log_timing

async def get_all_chats_with_counts(folder_name: str | None = None) -> List[dict]:
    """
    Devuelve todos los chats conocidos con conteo de videos (Async).
    Útil para la carpeta especial "Todos los canales".
    """
    async with aiosqlite.connect(DB_PATH) as db:
        log_timing("Iniciando consulta (chats)")
        chats_query = """
            SELECT
                c.chat_id,
                c.name,
                c.type,
                c.photo_id,
                c.username,
                c.last_message_date
            FROM chats c
            ORDER BY c.last_message_date IS NULL,
                     c.last_message_date DESC,
                     c.name COLLATE NOCASE
        """
        async with db.execute(chats_query) as cursor:
            chat_rows = await cursor.fetchall()

        log_timing("Consulta conteos indexados")
        counts_query = """
            SELECT chat_id, COUNT(id) AS indexed_videos
            FROM videos_telegram
            GROUP BY chat_id
        """
        async with db.execute(counts_query) as cursor:
            count_rows = await cursor.fetchall()
        counts_map = {r[0]: r[1] for r in count_rows}

        log_timing("Consulta conteos totales (scan)")
        totals_query = """
            SELECT chat_id, videos_count, scanned_at
            FROM chat_video_counts
        """
        async with db.execute(totals_query) as cursor:
            totals_rows = await cursor.fetchall()
        totals_map = {r[0]: (r[1], r[2]) for r in totals_rows}

        log_timing("Consulta folders por chat")
        folders_query = """
            SELECT chat_id, folder_id
            FROM chat_folders
        """
        async with db.execute(folders_query) as cursor:
            folders_rows = await cursor.fetchall()
        folders_map = {}
        for chat_id_fk, folder_id_fk in folders_rows:
            folders_map.setdefault(chat_id_fk, []).append(folder_id_fk)

    log_timing("Consulta realizada")
    items: List[dict] = []

    for row in chat_rows:
        chat_id, name, chat_type, photo_id, username, last_msg_dt = row
        indexed_videos = counts_map.get(chat_id, 0)
        total_videos, scanned_at = totals_map.get(chat_id, (0, None))

        # Excluir el canal de dump
        if CACHE_DUMP_VIDEOS_CHANNEL_ID and chat_id == CACHE_DUMP_VIDEOS_CHANNEL_ID:
            continue

        folders = folders_map.get(chat_id, [])

        display_name = name or f"Chat {chat_id}"
        info_text = chat_type or "cache"

        if last_msg_dt:
            try:
                dt = datetime.datetime.fromisoformat(last_msg_dt)
                info_text = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                info_text = last_msg_dt

        # Texto: indexados vs totales
        if total_videos:
            info_text = f"{indexed_videos} indexados / {total_videos} totales"
        else:
            info_text = f"{indexed_videos} indexados"

        # Marcar fecha de escaneo si existe
        if scanned_at:
            try:
                dt_scan = datetime.datetime.fromisoformat(scanned_at)
                info_text += f" · {dt_scan.strftime('%Y-%m-%d')}"
            except Exception:
                info_text += f" · {scanned_at}"

        telegram_link = None
        if username:
            telegram_link = f"https://t.me/{username}"

        link = f"/channel/{chat_id}?name={display_name}"
        if folder_name:
            link += f"&folder_id=-1&folder_name={folder_name}"

        photo_url = None
        if photo_id:
            photo_url = f"/api/photo/{photo_id}?tipo=grupo"

        items.append({
            "name": display_name,
            "count": info_text,
            "link": link,
            "type": "chat",
            "photo_id": photo_id,
            "photo_url": photo_url,
            "folder_id": -1,
            "folders": folders,
            "chat_id": chat_id,
            "username": username,
            "telegram_link": telegram_link,
            "indexed_videos": indexed_videos,
            "total_videos": total_videos,
            "scanned_at": scanned_at,
            "chat_type": chat_type,
            "last_message_date": last_msg_dt,
        })
        
    log_timing("Items agregados")
    return items


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
    log_timing("Consulta lista")
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
    
        log_timing("Items cargados")
    return items