"""
Rutas de media: Streaming inteligente, descargas background y Cache en RAM.
Refactorizado con Semáforo para evitar FloodWait en Thumbnails.
"""
import os
import tempfile
import asyncio
import aiofiles
import aiosqlite
from fastapi import APIRouter, Request, Header, HTTPException, BackgroundTasks, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, StreamingResponse
from pyrogram.errors import FloodWait

from config import TEMPLATES_DIR, THUMB_FOLDER, GRUPOS_THUMB_FOLDER, MAIN_TEMPLATE, DUMP_FOLDER, DB_PATH, CACHE_DUMP_VIDEOS_CHANNEL_ID, SMART_CACHE_ENABLED
from services import get_client, TelegramVideoSender, prefetch_channel_videos_to_ram, background_thumb_downloader
from services.thumb_worker_hibrido import _descargar_con_cliente
from utils import save_image_as_webp, convertir_tamano, formatear_miles
from database import db_get_video_messages

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# --- CONTROL DE CONCURRENCIA ---
# Limitamos a 3 descargas simultáneas de thumbnails para evitar auth.ExportAuthorization
thumb_download_sem = asyncio.Semaphore(3)

# --- CACHÉ EN MEMORIA ---
# Caché para evitar consultas repetidas a la BD
video_info_cache = {}
MAX_CACHE_SIZE = 1000

thumb_db_cache = {}

# --- UTILS DB ASYNC ---
async def get_video_info_from_db(chat_id: int, message_id: int):
    """
    Busca de forma ASÍNCRONA si el video ya está descargado por completo en disco.
    Usa caché en memoria para evitar consultas repetidas.
    """
    cache_key = f"{chat_id}:{message_id}"
    
    # 1. Intentar obtener de la caché
    if cache_key in video_info_cache:
        return video_info_cache[cache_key]
        
    # 2. Si no está en caché, consultar BD
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, ruta_local FROM videos_telegram WHERE chat_id = ? AND message_id = ?", 
                (chat_id, message_id)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    # Guardar en caché
                    result = (row[0], row[1])  # video_id, ruta_local
                    if len(video_info_cache) >= MAX_CACHE_SIZE:
                        # Limpiar caché si es muy grande
                        video_info_cache.clear()
                    video_info_cache[cache_key] = result
                    return result
    except Exception as e:
        print(f"Error DB info: {e}")
    
    # Guardar el negativo también en caché
    video_info_cache[cache_key] = (None, None)
    return None, None

def _format_duration(seconds):
    if not seconds:
        return ""
    try:
        mins, secs = divmod(int(seconds), 60)
        hours, mins = divmod(mins, 60)
        if hours:
            return f"{hours}:{mins:02d}:{secs:02d}"
        return f"{mins}:{secs:02d}"
    except Exception:
        return ""

def _build_page_links(page: int, total_pages: int):
    """
    Genera una lista de páginas con el patrón clásico 1 .. N.
    Reglas:
    - Si hay pocas páginas (<=12), las muestra todas.
    - Si el usuario está al inicio (página <= 8), muestra 1..10, elipsis y las últimas 2.
    - Si está al final (página >= total_pages-7), muestra 1,2, elipsis y las últimas 10.
    - En el medio, muestra 1,2, elipsis, una ventana centrada en la página (5 ítems), elipsis y las últimas 2.
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
        _push(None)  # elipsis
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

    # Limpiar posibles duplicados y ordenar, preservando elipsis
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

# --- BACKGROUND TASKS ---

async def _background_video_download(chat_id: int, message_id: int, video_id: str):
    """
    Descarga el video COMPLETO al disco SSD en segundo plano.
    """
    client = get_client()
    save_dir = os.path.join(DUMP_FOLDER, "videos", str(chat_id))
    os.makedirs(save_dir, exist_ok=True)
    filename = f"{video_id}.mp4"
    file_path = os.path.join(save_dir, filename)
    
    print(f"⬇️ [BG] Iniciando descarga completa a DISCO: {filename}")
    
    try:
        downloaded = await client.download_media(message_id, file_name=file_path)
        
        if downloaded:
            print(f"✅ [BG] Descarga en disco completada: {file_path}")
            # Usamos aiosqlite para la actualización final
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE videos_telegram SET ruta_local = ?, en_mega = 0 WHERE chat_id = ? AND message_id = ?", 
                    (file_path, chat_id, message_id)
                )
                await db.commit()
    except Exception as e:
        print(f"❌ [BG] Error descarga {video_id}: {e}")

# --- RUTAS ---

@router.get("/play/{chat_id}/{message_id}")
async def player_page(request: Request, chat_id: int, message_id: int):
    stream_url = f"/video_stream/{chat_id}/{message_id}"
    
    # DB call async
    video_id, local_path = await get_video_info_from_db(chat_id, message_id)
    is_downloaded = local_path and os.path.exists(local_path)
    
    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "view_type": "player",
        "stream_url": stream_url,
        "current_folder_name": "Reproductor",
        "parent_link": f"/channel/{chat_id}",
        "video_info": {"id": video_id, "chat_id": chat_id, "message_id": message_id},
        "is_downloaded": is_downloaded
    })

@router.get("/videos")
async def all_videos_with_thumbs_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=200),
    sort: str = Query("fecha"),
    direction: str = Query("desc"),
):
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

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute(
            "SELECT COUNT(*) AS cnt FROM videos_telegram WHERE has_thumb = 1  and oculto=0"
        ) as cursor:
            row = await cursor.fetchone()
            total_items = int((row["cnt"] if row else 0) or 0)
            print(f"Total de videos {total_items}")
            print(f"LIMIT:{(per_page)}")
            
        async with db.execute(
            """
            SELECT chat_id, message_id, nombre, caption, tamano_bytes, file_unique_id, file_id,
                   duracion, ancho, alto, mime_type, views, fecha_mensaje
            FROM videos_telegram
            WHERE has_thumb = 1   and oculto=0
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """.format(order_clause=order_clause),
            (per_page, offset),
        ) as cursor:
            rows = await cursor.fetchall()
           
            print(f"Total de videos a mostrar {len(rows)}")
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
        thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
        if not os.path.exists(thumb_path):
            continue

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
                "messages_count": messages_counts.get(video_id, 0),
            }
        )

    total_pages = max(1, (total_items + per_page - 1) // per_page) if total_items else 1
    has_prev = page > 1
    has_next = page < total_pages
    page_links = _build_page_links(page, total_pages)

    total_items_fmt = formatear_miles(total_items)
    total_pages_fmt = formatear_miles(total_pages)

    query_suffix = f"&sort={sort}&direction={direction}"

    return templates.TemplateResponse(
        MAIN_TEMPLATE,
        {
            "request": request,
            "items": items,
            "view_type": "files",
            "current_folder_name": "Videos",
            "current_folder_url": "/videos",
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
                "base_path": "/videos",
                "query_suffix": query_suffix,
                "sort": sort,
                "direction": direction,
            },
        },
    )

@router.get("/video_stream/{chat_id}/{message_id}")
async def video_stream(chat_id: int, message_id: int, range: str = Header(None)):
    """
    Streaming Híbrido: Disco -> RAM -> Telegram.
    """
    client = get_client()
    
    # DB call async
    video_id, local_path = await get_video_info_from_db(chat_id, message_id)
    
    sender = TelegramVideoSender(client, chat_id, message_id, video_id=video_id, local_path=local_path)
    await sender.setup()
    
    start, end = 0, sender.total_size - 1
    if range:
        try:
            r = range.replace("bytes=", "").split("-")
            start = int(r[0])
            if len(r) > 1 and r[1]:
                end = int(r[1])
        except Exception:
            pass
    
    if start >= sender.total_size:
        raise HTTPException(status_code=416, detail="Range not satisfiable")
    
    headers = sender.get_headers(start, end)
    return StreamingResponse(
        sender.stream_generator(start, end),
        status_code=206,
        headers=headers,
        media_type=headers["Content-Type"],
    )

@router.post("/api/download/{chat_id}/{message_id}")
async def download_video(chat_id: int, message_id: int, video_id: str, background_tasks: BackgroundTasks):
    """Botón 'Descargar' (Disco Completo)."""
    background_tasks.add_task(_background_video_download, chat_id, message_id, video_id)
    return {"status": "started", "message": "Descarga completa iniciada en 2do plano"}

@router.post("/api/prefetch/{chat_id}")
async def prefetch_channel(chat_id: int, background_tasks: BackgroundTasks):
    """Botón 'Cargar Grilla' (Manual)."""
    if not SMART_CACHE_ENABLED:
        return {"status": "disabled", "message": "SmartCache desactivado"}
    background_tasks.add_task(prefetch_channel_videos_to_ram, chat_id)
    return {"status": "started", "message": "Carga en RAM iniciada"}

@router.post("/api/thumbs/hibrido")
async def run_hybrid_thumb_worker(background_tasks: BackgroundTasks):
    """Lanza el worker híbrido de thumbnails (Bot + User) en 2do plano."""
    background_tasks.add_task(background_thumb_downloader)
    return {"status": "started", "message": "Worker híbrido de thumbnails iniciado"}

@router.get("/api/video/{video_id}/messages")
async def api_video_messages(video_id: str):
    # DB call async
    messages = await db_get_video_messages(video_id)
    return {"video_id": video_id, "messages": messages}

@router.get("/api/photo/{file_id}")
async def get_photo(file_id: str, tipo: str = "video", chat_id: int = None, video_id: str = None):
    """
    Obtiene la miniatura (cacheada o descargada) con protección anti-FloodWait.
    Versión optimizada con caché y menos pausas.
    """
    client = get_client()
    
    # Definir rutas según tipo (optimizado)
    if tipo == "grupo":
        target_folder = GRUPOS_THUMB_FOLDER
        filename = f"{file_id}.webp"
    elif chat_id and video_id:
        target_folder = os.path.join(THUMB_FOLDER, str(chat_id))
        os.makedirs(target_folder, exist_ok=True)
        filename = f"{video_id}.webp"
    else:
        target_folder = THUMB_FOLDER
        filename = f"{file_id}.webp"
        
    filepath = os.path.join(target_folder, filename)

    # 1. Si ya existe, devolver inmediatamente
    if os.path.exists(filepath):
        return FileResponse(filepath)

    # 2. Si no existe, descargar con SEMÁFORO y protección
    try:
        async with thumb_download_sem:
            # Si es thumbnail de VIDEO con chat_id + video_id, usar lógica híbrida
            if tipo == "video" and chat_id is not None and video_id is not None:
                # Usar caché para consultas BD repetidas
                if video_id in thumb_db_cache:
                    row = thumb_db_cache[video_id]
                else:
                    try:
                        async with aiosqlite.connect(DB_PATH) as db:
                            async with db.execute(
                                "SELECT chat_id, message_id, file_unique_id FROM videos_telegram WHERE id = ?",
                                (video_id,),
                            ) as cursor:
                                row = await cursor.fetchone()
                                # Guardar en caché (incluso si es None)
                                thumb_db_cache[video_id] = row
                                # Limpiar caché si es muy grande
                                if len(thumb_db_cache) > 1000:
                                    # Eliminar 20% de las entradas más antiguas
                                    keys_to_delete = list(thumb_db_cache.keys())[:200]
                                    for k in keys_to_delete:
                                        thumb_db_cache.pop(k, None)
                    except Exception as e:
                        row = None

                if row:
                    db_chat_id, message_id, file_unique_id = row
                    try:
                        res = await _descargar_con_cliente(
                            client,
                            db_chat_id,
                            message_id,
                            file_unique_id,
                            os.path.dirname(filepath),
                            filepath,
                            es_bot=False,
                        )
                        
                        if res is True and os.path.exists(filepath):
                            return FileResponse(filepath)
                    except Exception:
                        pass
                    # Si falla, continuamos al fallback con file_id
            
            # Fallback: descarga directa con file_id
            with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as tmp:
                temp_path = tmp.name

            try:
                downloaded_path = await client.download_media(file_id, file_name=temp_path)
                if downloaded_path:
                    # Convertir a WebP en thread aparte (más rápido)
                    await asyncio.to_thread(save_image_as_webp, downloaded_path, filepath)
                    # Limpieza
                    if os.path.exists(downloaded_path):
                        os.remove(downloaded_path)
                    return FileResponse(filepath)
                else:
                    os.remove(temp_path)
                    return {"error": "failed_download"}
            except FloodWait as e:
                os.remove(temp_path)
                return {"error": "flood_wait", "retry_after": e.value}
            except Exception:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return {"error": "download_failed"}

    except Exception as e:
        print(f"❌ Error general thumb: {e}")
        return {"error": "img_error"}

@router.get("/api/stats")
async def api_stats(limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
            columns = await cursor.fetchall()
            col_names = {c[1] for c in (columns or [])}
        has_dump_fail = "dump_fail" in col_names
        has_dump_message_id = "dump_message_id" in col_names

        async with db.execute("SELECT COUNT(*) FROM videos_telegram") as cursor:
            total_videos = (await cursor.fetchone() or [0])[0] or 0

        async with db.execute("SELECT COUNT(DISTINCT file_unique_id) FROM videos_telegram") as cursor:
            unique_videos = (await cursor.fetchone() or [0])[0] or 0

        async with db.execute("SELECT COUNT(*) FROM videos_telegram WHERE has_thumb = 0") as cursor:
            videos_sin_thumb = (await cursor.fetchone() or [0])[0] or 0

        async with db.execute("SELECT COUNT(*) FROM videos_telegram WHERE es_vertical = 1") as cursor:
            videos_verticales = (await cursor.fetchone() or [0])[0] or 0

        async with db.execute("SELECT COUNT(*) FROM videos_telegram WHERE duracion >= 3600") as cursor:
            videos_largos = (await cursor.fetchone() or [0])[0] or 0

        if has_dump_message_id:
            async with db.execute(
                """
                SELECT v.chat_id,
                       COALESCE(c.name, CAST(v.chat_id AS TEXT)) AS name,
                       c.username,
                       COUNT(*) AS sin_thumb,
                       (SELECT COUNT(*) FROM videos_telegram vt WHERE vt.chat_id = v.chat_id) AS total_videos,
                       (SELECT COUNT(*) FROM videos_telegram vt WHERE vt.chat_id = v.chat_id AND vt.dump_message_id IS NOT NULL) AS dump_videos
                FROM videos_telegram v
                LEFT JOIN chats c ON c.chat_id = v.chat_id
                WHERE v.has_thumb = 0
                  AND v.chat_id != ?
                GROUP BY v.chat_id, c.username, c.name
                ORDER BY sin_thumb DESC
                LIMIT ?
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, limit),
            ) as cursor:
                top_no_thumb_rows = await cursor.fetchall()
        else:
            async with db.execute(
                """
                SELECT v.chat_id,
                       COALESCE(c.name, CAST(v.chat_id AS TEXT)) AS name,
                       c.username,
                       COUNT(*) AS sin_thumb
                FROM videos_telegram v
                LEFT JOIN chats c ON c.chat_id = v.chat_id
                WHERE v.has_thumb = 0
                  AND v.chat_id != ?
                GROUP BY v.chat_id, c.username, c.name
                ORDER BY sin_thumb DESC
                LIMIT ?
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, limit),
            ) as cursor:
                top_no_thumb_rows = await cursor.fetchall()

        async with db.execute(
            """
            SELECT v.chat_id,
                   COALESCE(c.name, CAST(v.chat_id AS TEXT)) AS name,
                   c.username,
                   COUNT(*) AS videos
            FROM videos_telegram v
            LEFT JOIN chats c ON c.chat_id = v.chat_id
            WHERE v.chat_id != ?
            GROUP BY v.chat_id, c.username, c.name
            ORDER BY videos DESC
            LIMIT ?
            """,
            (CACHE_DUMP_VIDEOS_CHANNEL_ID, limit),
        ) as cursor:
            top_groups_rows = await cursor.fetchall()

        restricted_rows = []
        if has_dump_fail and has_dump_message_id:
            async with db.execute(
                """
                SELECT v.chat_id,
                       COALESCE(c.name, CAST(v.chat_id AS TEXT)) AS name,
                       c.username,
                       COUNT(*) AS blocked
                FROM videos_telegram v
                LEFT JOIN chats c ON c.chat_id = v.chat_id
                WHERE v.dump_fail = 1
                  AND v.dump_message_id IS NULL
                  AND v.chat_id != ?
                GROUP BY v.chat_id, c.username, c.name
                ORDER BY blocked DESC
                LIMIT ?
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, limit),
            ) as cursor:
                restricted_rows = await cursor.fetchall()

    def build_link(chat_id: int) -> str:
        # Usamos la ruta local de la app para abrir el canal/grupo
        return f"/channel/{chat_id}"

    top_groups = []
    for r in (top_groups_rows or []):
        chat_id, name, username, videos = r
        top_groups.append(
            {
                "chat_id": chat_id,
                "name": name,
                "username": username,
                "videos": videos,
                "telegram_link": build_link(chat_id),
            }
        )

    top_no_thumb_groups = []
    for r in (top_no_thumb_rows or []):
        chat_id, name, username, sin_thumb, *rest = r
        total_videos = rest[0] if rest else None
        dump_videos = rest[1] if len(rest) > 1 else None
        percent_dump = 0
        if total_videos and dump_videos is not None:
            try:
                percent_dump = round((dump_videos / total_videos) * 100, 1)
            except Exception:
                percent_dump = 0
        top_no_thumb_groups.append(
            {
                "chat_id": chat_id,
                "name": name,
                "username": username,
                "sin_thumb": sin_thumb,
                "total_videos": total_videos,
                "dump_videos": dump_videos,
                "dump_percentage": percent_dump,
                "telegram_link": build_link(chat_id),
            }
        )

    restricted_forward_groups = []
    for r in (restricted_rows or []):
        chat_id, name, username, blocked = r
        restricted_forward_groups.append(
            {
                "chat_id": chat_id,
                "name": name,
                "username": username,
                "blocked": blocked,
                "telegram_link": build_link(chat_id),
            }
        )

    return {
        "total_videos": total_videos,
        "unique_videos": unique_videos,
        "videos_sin_thumb": videos_sin_thumb,
        "videos_verticales": videos_verticales,
        "videos_largos_mas_1h": videos_largos,
        "top_groups": top_groups,
        "top_no_thumb_groups": top_no_thumb_groups,
        "restricted_forward_groups": restricted_forward_groups,
    }