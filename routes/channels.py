"""
Rutas de canales/chats con Pre-carga Automática en RAM.
Versión Async para DB (aiosqlite) con Background Tasks para no bloquear la UI.
"""
import os
import json
import asyncio
import aiosqlite  # Necesario para leer rápido la BD local
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.templating import Jinja2Templates
from pyrogram import enums

from config import TEMPLATES_DIR, JSON_FOLDER, MAIN_TEMPLATE, DB_PATH, SMART_CACHE_ENABLED
from services import get_client, prefetch_channel_videos_to_ram 
from database import (
    db_upsert_video, db_add_video_file_id, 
    db_get_chat, db_get_chat_folders,
    db_upsert_video_message
)
from utils import convertir_tamano

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

def _format_duration(seconds: int) -> str:
    if not seconds:
        return ""
    mins, secs = divmod(int(seconds), 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}:{mins:02d}:{secs:02d}"
    return f"{mins}:{secs:02d}"

async def get_local_videos(chat_id: int):
    """Obtiene rápidamente los videos ya guardados en la BD para mostrar algo al usuario."""
    videos = []
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            # Seleccionamos campos compatibles con la vista
            cursor = await db.execute("""
                SELECT message_id, nombre, tamano_bytes, file_unique_id, 
                       file_id, caption, duracion, ancho, alto, mime_type, 
                       views, outgoing, fecha_mensaje,
                       (
                           SELECT COUNT(*) FROM video_messages vm
                           WHERE vm.video_id = videos_telegram.file_unique_id
                       ) AS messages_count
                FROM videos_telegram 
                WHERE chat_id = ? 
                ORDER BY message_id DESC
            """, (chat_id,))
            rows = await cursor.fetchall()
            
            for r in rows:
                videos.append({
                    "name": r["nombre"],
                    "count": convertir_tamano(r["tamano_bytes"]),
                    "link": f"/play/{chat_id}/{r['message_id']}",
                    "type": "video",
                    "photo_id": r["file_id"],  # La BD no suele guardar thumb_id, se puede mejorar
                    "chat_id": chat_id,
                    "video_id": r["file_unique_id"],
                    "file_unique_id": r["file_unique_id"],
                    "caption": r["caption"],
                    "duration_text": _format_duration(r["duracion"]),
                    "file_size": r["tamano_bytes"],
                    "message_id": r["message_id"],
                    "views": r["views"],
                    "date": r["fecha_mensaje"],
                    "messages_count": r["messages_count"],
                })
    except Exception as e:
        print(f"⚠️ Error leyendo cache local: {e}")
    return videos

async def scan_channel_background(chat_id: int):
    """
    Tarea pesada que corre en segundo plano:
    1. Escanea historial de Telegram.
    2. Actualiza la BD.
    3. Genera JSON dump.
    4. Dispara el prefetch.
    """
    print(f"🕵️‍♂️ [Background] Iniciando escaneo profundo de {chat_id}...")
    client = get_client()
    raw_dump = []
    count_new = 0

    try:
        # Escaneo asíncrono de historial
        async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO):
            if m.video:
                v = m.video
                fn = v.file_name or f"Video {m.id}"
                msg_dict = json.loads(str(m))

                # Upsert Video
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
                await db_add_video_file_id(v.file_unique_id, v.file_id, v.file_unique_id, "scan")

                # Upsert Mensaje
                message_data = {
                    "video_id": v.file_unique_id,
                    "chat_id": chat_id,
                    "message_id": m.id,

                    "date": m.date.isoformat() if m.date else None,
                    "from_user": msg_dict.get("from_user"),
                    "media": msg_dict.get("media"),
                    "views": m.views or 0,
                    "forwards": getattr(m, "forwards", None),
                    "outgoing": m.outgoing,
                    "reply_to_message_id": getattr(m, "reply_to_message_id", None),
                    "forward_from_chat": msg_dict.get("forward_from_chat"),
                    "forward_from_message_id": msg_dict.get("forward_from_message_id"),
                    "forward_date": msg_dict.get("forward_date"),
                    "caption": msg_dict.get("caption"),
                    "caption_entities": msg_dict.get("caption_entities"),
                    "raw_json": msg_dict,
                }
                await db_upsert_video_message(message_data)
                
                # Guardamos el ID de mensaje explícito en el dump para referencia rápida
                msg_dict["message_id"] = m.id
                raw_dump.append(msg_dict)
                count_new += 1

        # Generar Dump JSON (opcional, legacy)
        if raw_dump:
            dump_data_channel = {
                "chat_id": chat_id,
                "items": [], # Ya no es vital llenarlo aquí si usamos BD
                "raw": raw_dump,
            }
            dump_path = os.path.join(JSON_FOLDER, f"raw_dump_{chat_id}.json")
            with open(dump_path, "w", encoding="utf-8") as f:
                json.dump(dump_data_channel, f, indent=4, ensure_ascii=False)

        print(f"✅ [Background] Escaneo finalizado para {chat_id}. {count_new} videos procesados.")
        
        # Al terminar el escaneo, disparamos el prefetch (SmartCache)
        if SMART_CACHE_ENABLED and count_new > 0:
            await prefetch_channel_videos_to_ram(chat_id)

    except Exception as e:
        print(f"❌ [Background] Error escaneando {chat_id}: {e}")

@router.get("/channel/{chat_id}")
async def ver_canal(
    request: Request,
    chat_id: int,
    background_tasks: BackgroundTasks, # <--- Inyección de dependencia vital
    name: str | None = None,
    folder_id: int | None = None,
    folder_name: str | None = None,
):
    """Vista de canal no bloqueante."""
    
    # 1. Recuperar datos locales (RÁPIDO)
    # Esto asegura que el usuario vea algo inmediatamente si ya entró antes.
    videos_locales = await get_local_videos(chat_id)
    
    # 2. Programar escaneo en segundo plano (FIRE AND FORGET)
    # Esto evita que la página se quede "pegada" cargando.
    background_tasks.add_task(scan_channel_background, chat_id)

    # 3. Lógica de UI (Nombres, carpetas, breadcrumbs)
    chat_meta = await db_get_chat(chat_id)
    
    if (not name or name == "Chat") and chat_meta and chat_meta.get("name"):
        name = chat_meta["name"]
    if not name:
        name = f"Chat {chat_id}"

    if folder_id is None:
        try: 
            folder_ids = await db_get_chat_folders(chat_id)
        except: 
            folder_ids = []
        if folder_ids: folder_id = folder_ids[0]

    if not folder_name:
        if folder_id == 0: folder_name = "Inbox"
        elif folder_id == 1: folder_name = "Archivados"

    if folder_id is not None and folder_name:
        current_folder_name = folder_name
        current_folder_url = f"/folder/{folder_id}?name={folder_name}"
        parent_link = current_folder_url
    else:
        current_folder_name = "Inicio"
        current_folder_url = "/"
        parent_link = "javascript:history.back()"

    # 4. Responder inmediatamente
    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": videos_locales, # Mostramos lo que tenemos ya cacheado
        "view_type": "files",
        "current_folder_name": current_folder_name,
        "current_folder_url": current_folder_url,
        "current_channel_name": name,
        "parent_link": parent_link,
        "is_scanning": True # Flag opcional para mostrar un spinner en el frontend
    })