"""
Rutas de canales/chats con Pre-carga Automática en RAM.
Versión Async para DB (aiosqlite) con Background Tasks para no bloquear la UI.
"""
import os
import json
import asyncio
import datetime
import aiosqlite  # Necesario para leer rápido la BD local
from fastapi import APIRouter, Request, BackgroundTasks, Query, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pyrogram import enums

from config import TEMPLATES_DIR, JSON_FOLDER, MAIN_TEMPLATE, DB_PATH, SMART_CACHE_ENABLED
from database.connection import get_db
from services import get_client, prefetch_channel_videos_to_ram, background_thumb_downloader
from database import (
    db_upsert_video, db_add_video_file_id, 
    db_get_chat, db_get_chat_folders, db_get_chat_scan_meta,
    db_upsert_video_message, db_count_videos_by_chat, db_upsert_chat_video_count
)
from utils import convertir_tamano, formatear_miles, ws_manager, log_timing

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

async def _get_telegram_meta(chat_id: int) -> dict:
    """
    Recupera metadatos directos desde Telegram sin descargar contenido
    (solo get_chat, no indexa ni baja medios).
    """
    meta: dict = {}
    try:
        client = get_client()
        tg_chat = await client.get_chat(chat_id)
        meta = {
            "title": getattr(tg_chat, "title", None),
            "username": getattr(tg_chat, "username", None),
            "type": str(getattr(tg_chat, "type", None)),
            "description": getattr(tg_chat, "description", None),
            "members_count": getattr(tg_chat, "members_count", None),
            "dc_id": getattr(tg_chat, "dc_id", None),
            "is_verified": getattr(tg_chat, "is_verified", None),
            "is_scam": getattr(tg_chat, "is_scam", None),
            "is_fake": getattr(tg_chat, "is_fake", None),
            "is_restricted": getattr(tg_chat, "is_restricted", None),
            "restriction_reason": getattr(tg_chat, "restriction_reason", None),
        }
    except Exception:
        # Si falla la conexión con Telegram seguimos con los datos locales
        meta = {}
    return meta

@router.get("/api/channel/{chat_id}/info")
async def api_channel_info(chat_id: int):
    """Devuelve metadatos básicos y conteo de videos del canal/chat."""
    log_timing(f" Iniciando endpoint /api/channel/{chat_id}/info..")
    chat = await db_get_chat(chat_id)
    if not chat:
        raise HTTPException(status_code=404, detail="Chat no encontrado")

    indexed = await db_count_videos_by_chat(chat_id)
    scan_meta = await db_get_chat_scan_meta(chat_id) or {}
    total = scan_meta.get("videos_count") if scan_meta.get("videos_count") is not None else indexed
    scanned_at = scan_meta.get("scanned_at") or chat.get("last_message_date")

    telegram_meta = await _get_telegram_meta(chat_id)
    name = chat.get("name") or telegram_meta.get("title")
    username = chat.get("username") or telegram_meta.get("username")
    chat_type = chat.get("type") or telegram_meta.get("type")

    result = {
        "chat_id": chat_id,
        "name": name,
        "type": chat_type,
        "username": username,
        "indexed_videos": indexed,
        "total_videos": total,
        "scanned_at": scanned_at,
        "last_message_date": chat.get("last_message_date"),
        "duplicados": scan_meta.get("duplicados"),
        "description": telegram_meta.get("description"),
        "members_count": telegram_meta.get("members_count"),
        "dc_id": telegram_meta.get("dc_id"),
        "is_verified": telegram_meta.get("is_verified"),
        "is_scam": telegram_meta.get("is_scam"),
        "is_fake": telegram_meta.get("is_fake"),
        "is_restricted": telegram_meta.get("is_restricted"),
        "restriction_reason": telegram_meta.get("restriction_reason"),
    }
    log_timing(f"Endpoint /api/channel/{chat_id}/info terminado")
    return result

@router.post("/api/channel/{chat_id}/scan")
async def api_channel_scan(chat_id: int, background_tasks: BackgroundTasks):
    """Lanza el escaneo solo para indexar metadatos (sin bots/thumbs)."""
    background_tasks.add_task(scan_channel_background, chat_id, False)
    return JSONResponse({"status": "scheduled"})

@router.get("/api/channel/{chat_id}/videos")
async def api_channel_videos(
    chat_id: int,
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=200),
    sort: str = Query("fecha"),
    direction: str = Query("desc"),
):
    """
    Devuelve videos ya indexados de un canal/chat (solo lectura, sin disparar indexación).
    """
    log_timing(f" Iniciando endpoint /api/channel/{chat_id}/videos..")
    videos_locales = await get_local_videos(chat_id)

    valid_sorts = {
        "fecha": ("date", True),
        "nombre": ("name", False),
        "duracion": ("duration_seconds", False),
    }
    sort_field, _ = valid_sorts.get(sort, ("date", True))
    direction = direction.lower()
    reverse = direction != "asc"

    if sort_field == "date":
        videos_locales = sorted(videos_locales, key=lambda x: x.get("date") or "", reverse=reverse)
    else:
        videos_locales = sorted(videos_locales, key=lambda x: x.get(sort_field) or 0, reverse=reverse)

    total_items = len(videos_locales)
    per_page = max(1, min(200, per_page))
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    items_paged = videos_locales[offset : offset + per_page]

    pagination = {
        "page": page,
        "per_page": per_page,
        "total_items": total_items,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": max(1, page - 1),
        "next_page": min(total_pages, page + 1),
        "sort": sort,
        "direction": direction,
    }
    log_timing(f"Endpoint /api/channel/{chat_id}/videos terminado")
    return {"items": items_paged, "pagination": pagination}

def _build_page_links(page: int, total_pages: int):
    """
    Copiado de routes.media: genera lista de páginas con elipsis.
    """
    if total_pages <= 12:
        return list(range(1, total_pages + 1))

    pages = []

    def _push(n):
        if n is None:
            pages.append(None)
            return
        if 1 <= n <= total_pages and n not in pages:
            pages.append(n)

    if page <= 8:
        for n in range(1, 11):
            _push(n)
        _push(None)
        _push(total_pages - 1)
        _push(total_pages)
    elif page >= total_pages - 7:
        _push(1)
        _push(2)
        _push(None)
        for n in range(total_pages - 9, total_pages + 1):
            _push(n)
    else:
        _push(1)
        _push(2)
        _push(None)
        for n in range(page - 2, page + 3):
            _push(n)
        _push(None)
        _push(total_pages - 1)
        _push(total_pages)

    cleaned = []
    prev_num = None
    for n in pages:
        if n is None:
            if cleaned and cleaned[-1] is not None:
                cleaned.append(None)
            prev_num = None
        else:
            if n != prev_num:
                cleaned.append(n)
                prev_num = n
    if cleaned and cleaned[-1] is None:
        cleaned.pop()
    return cleaned

async def _thumb_poller(stop_event: asyncio.Event):
    """Dispara el worker de thumbs en bucle mientras corre el escaneo."""
    while not stop_event.is_set():
        try:
            await background_thumb_downloader()
        except asyncio.CancelledError:
            break
        except Exception:
            pass
        await asyncio.sleep(5)
    # Última pasada al terminar el escaneo
    try:
        await background_thumb_downloader()
    except asyncio.CancelledError:
        pass
    except Exception:
        pass

async def get_local_videos(chat_id: int):
    """Obtiene rápidamente los videos ya guardados en la BD para mostrar algo al usuario."""
    videos = []
    try:
        async with get_db() as db:
            db.row_factory = aiosqlite.Row
            # Seleccionamos campos compatibles con la vista
            cursor = await db.execute("""
                SELECT message_id, nombre, tamano_bytes, file_unique_id, 
                       file_id, caption, duracion, ancho, alto, mime_type, 
                       views, outgoing, fecha_mensaje, has_thumb,
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
                    "duration_seconds": r["duracion"],
                    "file_size": r["tamano_bytes"],
                    "message_id": r["message_id"],
                    "views": r["views"],
                    "date": r["fecha_mensaje"],
                    "messages_count": r["messages_count"],
                    "has_thumb": r["has_thumb"]
                })
    except Exception as e:
        print(f"⚠️ Error leyendo cache local: {e}")
    return videos

async def scan_channel_background(chat_id: int, run_thumb_worker: bool = True):
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
    duplicates_count = 0
    seen_files: set[str] = set()
    stop_event = asyncio.Event()
    thumb_task = None
    if run_thumb_worker:
        thumb_task = asyncio.create_task(_thumb_poller(stop_event))

    try:
        # Escaneo asíncrono de historial
        async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO):
            if m.video:
                v = m.video
                fn = v.file_name or f"Video {m.id}"
                msg_dict = json.loads(str(m))

                # Duplicados: si el file_unique_id ya se vio, contamos duplicado y seguimos upsert
                if v.file_unique_id in seen_files:
                    duplicates_count += 1
                else:
                    seen_files.add(v.file_unique_id)

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
        
        try:
            indexed_final = await db_count_videos_by_chat(chat_id)
            scanned_at = datetime.datetime.utcnow().isoformat()
            await db_upsert_chat_video_count(
                chat_id,
                indexed_final,          # videos_count
                scanned_at,
                duplicates_count,
                indexados=indexed_final
            )
            await ws_manager.broadcast_event({
                "type": "scan_done",
                "chat_id": chat_id,
                "indexed_videos": indexed_final,
                "total_videos": indexed_final,
                "duplicados": duplicates_count,
            })
        except Exception:
            pass

    except Exception as e:
        print(f"❌ [Background] Error escaneando {chat_id}: {e}")
    finally:
        stop_event.set()
        if thumb_task:
            try:
                await thumb_task
            except asyncio.CancelledError:
                pass

@router.get("/channel/{chat_id}")
async def ver_canal(
    request: Request,
    chat_id: int,
    background_tasks: BackgroundTasks, 
    name: str | None = None,
    folder_id: int | None = None,
    folder_name: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=200),
    sort: str = Query("fecha"),
    direction: str = Query("desc"),
):
    """Vista de canal no bloqueante."""
    log_timing(f" Iniciando endpoint /channel/{chat_id}..")
    
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

    # 4. Orden y paginación básica (mantener selector visible y navegable)
    valid_sorts = {
        "fecha": ("date", True),
        "nombre": ("name", False),
        "duracion": ("duration_seconds", False),
    }
    sort_field, _ = valid_sorts.get(sort, ("date", True))
    direction = direction.lower()
    reverse = direction != "asc"

    if sort_field == "date":
        videos_locales = sorted(videos_locales, key=lambda x: x.get("date") or "", reverse=reverse)
    else:
        videos_locales = sorted(videos_locales, key=lambda x: x.get(sort_field) or 0, reverse=reverse)

    total_items = len(videos_locales)
    per_page = max(1, min(200, per_page))
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    items_paged = videos_locales[offset : offset + per_page]

    base_path = f"/channel/{chat_id}"
    query_suffix = ""
    if name:
        query_suffix += f"&name={name}"
    if folder_id is not None and folder_name:
        query_suffix += f"&folder_id={folder_id}&folder_name={folder_name}"
    if sort:
        query_suffix += f"&sort={sort}"
    if direction:
        query_suffix += f"&direction={direction}"

    pagination = {
        "page": page,
        "page_fmt": formatear_miles(page),
        "per_page": per_page,
        "total_items": total_items,
        "total_items_fmt": formatear_miles(total_items),
        "total_pages": total_pages,
        "total_pages_fmt": formatear_miles(total_pages),
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": max(1, page - 1),
        "next_page": min(total_pages, page + 1),
        "base_path": base_path,
        "query_suffix": query_suffix,
        "sort": sort,
        "direction": direction,
        "pages": _build_page_links(page, total_pages),
    }

    # 5. Responder inmediatamente
    result = templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": items_paged, # Mostramos lo que tenemos ya cacheado paginado

        "view_type": "files",
        "current_folder_name": current_folder_name,
        "current_folder_url": current_folder_url,
        "current_channel_name": name,
        "current_channel_id": chat_id,
        "parent_link": parent_link,
        "is_scanning": True, # Flag opcional para mostrar un spinner en el frontend
        "pagination": pagination,
    })
    log_timing(f"Endpoint /channel/{chat_id} terminado")
    return result