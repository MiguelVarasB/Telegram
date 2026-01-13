import os
import platform
import subprocess
import aiosqlite
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.templating import Jinja2Templates

from config import TEMPLATES_DIR, THUMB_FOLDER, MAIN_TEMPLATE, DB_PATH,LIMIT_PER_PAGE
from utils import convertir_tamano, formatear_miles, log_timing
from .media_common import _build_page_links, _format_duration, get_video_info_from_db

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

@router.get("/play/{chat_id}/{message_id}")
async def player_page(request: Request, chat_id: int, message_id: int, file_unique_id: str | None = Query(None)):
    log_timing(f" Iniciando endpoint /play/{chat_id}/{message_id}..")
    base_stream = f"/video_stream/{chat_id}/{message_id}"
    stream_url = f"{base_stream}?file_unique_id={file_unique_id}" if file_unique_id else base_stream

    # DB call async
    video_id, local_path = await get_video_info_from_db(chat_id, message_id, file_unique_id)
    is_downloaded = local_path and os.path.exists(local_path)

    result = templates.TemplateResponse(
        MAIN_TEMPLATE,
        {
            "request": request,
            "view_type": "player",
            "stream_url": stream_url,
            "current_folder_name": "Reproductor",
            "parent_link": f"/channel/{chat_id}",
            "video_info": {"id": video_id, "chat_id": chat_id, "message_id": message_id},
            "is_downloaded": is_downloaded,
        },
    )
    log_timing(f"Endpoint /play/{chat_id}/{message_id} terminado")
    return result


@router.get("/playwindows/{chat_id}/{message_id}")
async def player_windows(chat_id: int, message_id: int, file_unique_id: str | None = Query(None)):
    """
    Abre el video descargado con el reproductor predeterminado de Windows.
    """
    if platform.system() != "Windows":
        raise HTTPException(status_code=400, detail="Reproducción local solo disponible en Windows.")

    _, local_path = await get_video_info_from_db(chat_id, message_id, file_unique_id)
    if not local_path or not os.path.exists(local_path):
        raise HTTPException(status_code=404, detail="El video no está descargado en el servidor.")

    try:
        os.startfile(local_path)  # type: ignore[attr-defined]
    except Exception:
        try:
            subprocess.Popen(["cmd", "/c", "start", "", local_path], shell=False)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"No se pudo abrir el video: {e}")

    return {"status": "ok", "message": "Abriendo en el reproductor predeterminado de Windows."}


async def _build_videos_page(
    request: Request,
    page: int,
    per_page: int,
    sort: str,
    direction: str,
    base_path: str,
    title: str,
    extra_where: str = "",
    extra_params: tuple = (),
    view_type: str = "files",
    thumb_filter: str = "con",
    buscar: str = "",
):
    print("Buscando videos...")   
   
    offset = (page - 1) * per_page

    valid_sorts = {
        "fecha": "fecha_mensaje",
        "nombre": "nombre",
        "duracion": "duracion",
    }
    sort_key = valid_sorts.get(sort, "fecha_mensaje")
    direction = direction.lower()
    if direction not in ("asc", "desc"):
        direction = "desc"

    order_clause = f"{sort_key} {direction.upper()}, message_id DESC"

    where_clause = "WHERE oculto = 0"
    if thumb_filter == "con":
        where_clause = f"{where_clause} AND has_thumb > 0"
    elif thumb_filter == "sin":
        where_clause = f"{where_clause} AND has_thumb = 0"
    if extra_where:
        where_clause = f"{where_clause} AND {extra_where}"
    if buscar:
        where_clause = f"{where_clause} AND (nombre LIKE ? OR caption LIKE ? or file_unique_id LIKE ?)"
        extra_params = (*extra_params, f"%{buscar}%", f"%{buscar}%", f"%{buscar}%")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute(
            f"SELECT COUNT(*) AS cnt FROM videos_telegram {where_clause}",
            extra_params,
        ) as cursor:
            row = await cursor.fetchone()
            total_items = int((row["cnt"] if row else 0) or 0)
        
        async with db.execute(
            f"""
            SELECT vt.chat_id,
                   vt.message_id,
                   vt.nombre,
                   vt.caption,
                   vt.tamano_bytes,
                   vt.file_unique_id,
                   vt.file_id,
                   vt.duracion,
                   vt.ancho,
                   vt.alto,
                   vt.mime_type,
                   vt.views,
                   vt.fecha_mensaje,
                   vt.watch_later,
                   vt.oculto,
                   vt.has_thumb,
                   vt.dump_message_id,
                   vt.ruta_local,
                   c.name AS chat_name,
                   c.username AS chat_username
            FROM videos_telegram vt
            LEFT JOIN chats c ON c.chat_id = vt.chat_id
            {where_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """,
            (*extra_params, per_page, offset),
        ) as cursor:
            rows = await cursor.fetchall()

        print(f"""
            SELECT chat_id
            FROM videos_telegram
            {where_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """)
        # --- Conteo de mensajes por video (batch para evitar 1M+ subqueries) ---
        messages_counts = {}
        video_ids = [r["file_unique_id"] for r in (rows or []) if r["file_unique_id"]]

        if video_ids:
            placeholders = ",".join(["?"] * len(video_ids))
            query = f"""
                SELECT video_id, COUNT(*) AS cnt
                FROM video_messages
                WHERE video_id IN ({placeholders})
                GROUP BY video_id
            """
            async with db.execute(query, video_ids) as cursor:
                for video_id, cnt in await cursor.fetchall():
                    messages_counts[video_id] = cnt

    items = []
    for r in (rows or []):
        video_id = r["file_unique_id"]
        chat_id = r["chat_id"]
        local_path = r["ruta_local"]
        is_downloaded = bool(local_path and os.path.exists(local_path))

        items.append(
            {
                "name": r["nombre"] or f"Video {r['message_id']}",
                "count": convertir_tamano(r["tamano_bytes"]),
                "link": f"/play/{chat_id}/{r['message_id']}",
                "type": "video",
                "photo_id": r["file_id"],
                "chat_id": chat_id,
                "video_id": video_id,
                "file_unique_id": video_id,
                "caption": r["caption"],
                "duration_text": _format_duration(r["duracion"]),
                "file_size": r["tamano_bytes"],
                "message_id": r["message_id"],
                "views": r["views"],
                "date": r["fecha_mensaje"],
                "width": r["ancho"],
                "height": r["alto"],
                "mime_type": r["mime_type"],
                "watch_later": bool(r["watch_later"]),
                "messages_count": messages_counts.get(video_id, 0),
                "oculto": (r["oculto"]),
                "has_thumb": (r["has_thumb"]),
                "dump_message_id": r["dump_message_id"],
                "local_path": local_path,
                "downloaded": is_downloaded,
                "chat_name": r["chat_name"],
                "chat_username": r["chat_username"],
            }
        )

    total_pages = max(1, (total_items + per_page - 1) // per_page) if total_items else 1
    has_prev = page > 1
    has_next = page < total_pages
    page_links = _build_page_links(page, total_pages)

    total_items_fmt = formatear_miles(total_items)
    total_pages_fmt = formatear_miles(total_pages)

    # Mantener todos los filtros en los enlaces de paginación
    query_suffix = (
        f"&sort={sort}"
        f"&direction={direction}"
        f"&thumb_filter={thumb_filter}"
        f"&buscar={buscar}"
    )

    return templates.TemplateResponse(
        MAIN_TEMPLATE,
        {
            "request": request,
            "items": items,
            "view_type": view_type,
            "current_folder_name": title,
            "current_folder_url": base_path,
            "current_channel_name": None,
            "parent_link": "/",
            "total_items": total_items,
            "total_items_fmt": total_items_fmt,
            "pagination": {
                "page": page,
                "page_fmt": formatear_miles(page),
                "per_page": per_page,
                "total_items": total_items,
                "total_items_fmt": total_items_fmt,
                "total_pages": total_pages,
                "total_pages_fmt": total_pages_fmt,
                "has_prev": has_prev,
                "has_next": has_next,
                "prev_page": page - 1,
                "next_page": page + 1,
                "base_path": base_path,
                "query_suffix": query_suffix,
                "sort": sort,
                "direction": direction,
                "pages": page_links,
                "thumb_filter": thumb_filter,
                "buscar": buscar,
            },
        },
    )

#/videos?page=1&direction=desc&per_page=11&sort=duracion&direction=desc&thumb_filter=sin
@router.get("/videos")
async def all_videos_with_thumbs_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(LIMIT_PER_PAGE, ge=1, le=200),
    sort: str = Query("fecha"),
    direction: str = Query("desc"),
    thumb_filter: str = Query("con"),
    buscar: str = Query(""),
):
    log_timing(f" Iniciando endpoint /videos..")
    result = await _build_videos_page(
        request,
        page,
        per_page,
        sort,
        direction,
        base_path="/videos",
        title="Videos",
        view_type="files",
        thumb_filter=thumb_filter,
        buscar =buscar,
    )
    log_timing(f"Endpoint /videos terminado")
    return result


@router.get("/watch_later")
async def watch_later_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(LIMIT_PER_PAGE, ge=1, le=200),
    sort: str = Query("fecha"),
    direction: str = Query("desc"),
):
    log_timing(" Iniciando endpoint /watch_later..")
    result = await _build_videos_page(
        request,
        page,
        per_page,
        sort,
        direction,
        base_path="/watch_later",
        title="Ver más tarde",
        extra_where="watch_later = 1",
        view_type="files",
    )
    log_timing("Endpoint /watch_later terminado")
    return result
