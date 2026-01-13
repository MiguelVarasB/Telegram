import os
import tempfile
import asyncio

import aiosqlite
from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import FileResponse
from PIL import UnidentifiedImageError
from pyrogram.errors import FloodWait, FileReferenceExpired

from config import THUMB_FOLDER, GRUPOS_THUMB_FOLDER, DB_PATH, CACHE_DUMP_VIDEOS_CHANNEL_ID
from services import get_client
from services.thumb_worker_hibrido import _descargar_con_cliente
from utils import save_image_as_webp,log_timing
from database import db_get_video_messages
from .media_common import thumb_download_sem, thumb_db_cache

router = APIRouter()


@router.get("/api/video/{video_id}/messages")
async def api_video_messages(video_id: str):
    log_timing(f" Iniciando endpoint /api/video/{video_id}/messages..")
    # DB call async
    messages = await db_get_video_messages(video_id)
    log_timing(f"Endpoint /api/video/{video_id}/messages terminado")
    return {"video_id": video_id, "messages": messages}


@router.post("/api/video/{video_id}/watch_later")
async def toggle_watch_later(video_id: str, value: bool = Body(..., embed=True)):
    """Marca o desmarca un video como 'ver más tarde'."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE videos_telegram SET watch_later = ? WHERE id = ?",
                (1 if value else 0, video_id),
            )
            await db.commit()
            async with db.execute(
                "SELECT watch_later FROM videos_telegram WHERE id = ?", (video_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video no encontrado")
                return {"video_id": video_id, "watch_later": bool(row[0])}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al actualizar watch_later: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar estado")


@router.get("/api/photo/{file_id}")
async def get_photo(
    file_id: str,
    tipo: str = "video",
    chat_id: int = None,
    video_id: str = None,
    has_thumb: str = "0",
):
    """
    Obtiene la miniatura con manejo robusto de FileReferenceExpired e integridad de imagen.
    """
    client = get_client()

    # 1. Rutas
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

    # Normalizamos has_thumb (evita 422 si viene vacío)
    try:
        has_thumb_int = int(has_thumb) if str(has_thumb).strip() != "" else 0
    except Exception:
        has_thumb_int = 0

    # 2. Cache
    if os.path.exists(filepath):
        # Si la miniatura existe pero la DB aún marca has_thumb=0, actualizar bandera.
        if has_thumb_int == 0 and video_id:
            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE videos_telegram SET has_thumb = 1 WHERE id = ?",
                        (video_id,),
                    )
                    await db.commit()
                    print(f"✅ Se actualizó has_thumb para {video_id}")
            except Exception as e:
                print(f"⚠️ No se pudo actualizar has_thumb para {video_id}: {e}")
        return FileResponse(filepath)

    async with thumb_download_sem:
        row_info = None

        # 3. Estrategia Híbrida (Worker / Cache DB)
        if tipo == "video" and chat_id is not None and video_id is not None:
            if video_id in thumb_db_cache:
                row = thumb_db_cache[video_id]
            else:
                try:
                    async with aiosqlite.connect(DB_PATH) as db:
                        async with db.execute(
                            "SELECT chat_id, message_id, dump_message_id, file_unique_id FROM videos_telegram WHERE id = ?",
                            (video_id,),
                        ) as c:
                            row = await c.fetchone()
                            thumb_db_cache[video_id] = row
                except Exception:
                    row = None
            row_info = row

            if row:
                try:
                    db_chat, msg_id, dump_msg, f_uniq = row
                    res = await _descargar_con_cliente(
                        client,
                        CACHE_DUMP_VIDEOS_CHANNEL_ID if dump_msg else db_chat,
                        dump_msg or msg_id,
                        f_uniq,
                        os.path.dirname(filepath),
                        filepath,
                        es_bot=False,
                    )
                    if res is True and os.path.exists(filepath):
                        return FileResponse(filepath)
                except Exception:
                    pass

        with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as tmp:
            temp_path = tmp.name

        # --- LOGICA DE OBTENCIÓN DESDE MENSAJE (REFRESCO / PRIMERA VEZ) ---
        async def _download_thumb_from_message() -> bool:
            """Obtiene el mensaje fresco y descarga SOLO el thumbnail (no el video)."""
            nonlocal row_info
            try:
                # Si no tenemos info, buscarla
                if row_info is None and video_id and chat_id:
                    async with aiosqlite.connect(DB_PATH) as db:
                        async with db.execute(
                            "SELECT chat_id, message_id, dump_message_id FROM videos_telegram WHERE id = ?",
                            (video_id,),
                        ) as c:
                            row_info = await c.fetchone()

                if row_info:
                    db_chat, msg_id, dump_msg, *_ = row_info
                    chat_target = CACHE_DUMP_VIDEOS_CHANNEL_ID if dump_msg else db_chat
                    msg_target = dump_msg or msg_id

                    # 1. Obtener mensaje
                    msg = await client.get_messages(chat_target, msg_target)
                    if not msg or msg.empty:
                        return False

                    media = getattr(msg, "video", None) or getattr(msg, "photo", None) or getattr(msg, "document", None)
                    if not media:
                        return False

                    # 2. SELECCIÓN INTELIGENTE (Evitar descargar video)
                    obj_to_download = None
                    thumbs = getattr(media, "thumbs", None)

                    if thumbs and len(thumbs) > 0:
                        obj_to_download = thumbs[0]  # Thumbnail pequeño
                    elif getattr(media, "file_id", "") and not getattr(media, "duration", None):
                        # Es una foto real (no video), ok descargar
                        obj_to_download = media

                    if not obj_to_download:
                        print(f"⚠️ El mensaje {msg_id} no tiene thumbnail. Saltando descarga de video.")
                        return False

                    # 3. Descargar objeto específico
                    new_path = await client.download_media(obj_to_download, file_name=temp_path)
                    if new_path:
                        try:
                            await asyncio.to_thread(save_image_as_webp, new_path, filepath)
                            return True
                        except Exception:
                            return False
                        finally:
                            if os.path.exists(new_path):
                                os.remove(new_path)
            except Exception as e_dl:
                print(f"⚠️ Error descarga thumb msg: {e_dl}")
            return False

        # --- ESTRATEGIA PRINCIPAL ---

        # Si es un video conocido de la DB, NO intentamos descargar el file_id directo
        # porque la DB guarda el file_id del VIDEO, no del thumb.
        should_skip_direct = tipo == "video" and video_id is not None

        if should_skip_direct:
            # Saltamos directo a buscar en el mensaje
            if await _download_thumb_from_message():
                return FileResponse(filepath)
            return {"error": "thumb_not_found_in_msg"}

        # Si NO es un video (es un chat, grupo, o algo genérico), probamos directo
        try:
            path = await client.download_media(file_id, file_name=temp_path)
            if path:
                try:
                    await asyncio.to_thread(save_image_as_webp, path, filepath)
                    return FileResponse(filepath)
                except UnidentifiedImageError:
                    print("❌ Imagen corrupta (posible video). Intentando recuperar...")
                    if await _download_thumb_from_message():
                        return FileResponse(filepath)
                    return {"error": "corrupted_image"}
                finally:
                    if os.path.exists(path):
                        os.remove(path)

        except FileReferenceExpired as e_expired:
            print("🔄 Link caducado. Refrescando...")
            if await _download_thumb_from_message():
                return FileResponse(filepath)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return {"error": "expired", "details": str(e_expired)}

        except FloodWait as e_flood:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return {"error": "flood_wait", "retry_after": e_flood.value}

        except Exception as e_gen:
            print(f"💥 Error genérico: {e_gen}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return {"error": "server_error"}

    return {"error": "unknown_state"}


@router.get("/api/stats")
async def api_stats(limit: int = 10):
    log_timing(f" Iniciando endpoint /api/stats..")
    EXCLUDE_CHAT_ID = -1003512635282
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
            columns = await cursor.fetchall()
            col_names = {c[1] for c in (columns or [])}
        has_dump_fail = "dump_fail" in col_names
        has_dump_message_id = "dump_message_id" in col_names

        # Totales rápidos desde tabla resumida si existe
        async with db.execute(
            """
            SELECT
                SUM(videos_count) AS total_videos,
                SUM(videos_count - COALESCE(duplicados, 0)) AS unique_videos_aprox,
                SUM(COALESCE(duplicados, 0)) AS duplicados_total,
                SUM(COALESCE(indexados, 0)) AS indexados_total
            FROM chat_video_counts
            WHERE chat_id != ?
            """
        , (EXCLUDE_CHAT_ID,)) as cursor:
            row = await cursor.fetchone() or [0, 0, 0, 0]
            (
                total_videos,
                unique_videos,
                duplicados_total,
                indexados_total,
            ) = [r or 0 for r in row]

        # Conteos globales optimizados con subconsultas para usar índices existentes
        async with db.execute(
            """
            SELECT 
                (SELECT COUNT(*) FROM videos_telegram WHERE has_thumb = 0 AND chat_id != ?) AS videos_sin_thumb,
                (SELECT COUNT(*) FROM videos_telegram WHERE es_vertical = 1 AND chat_id != ?) AS videos_verticales,
                (SELECT COUNT(*) FROM videos_telegram WHERE duracion >= 3600 AND chat_id != ?) AS videos_largos
            """,
            (EXCLUDE_CHAT_ID, EXCLUDE_CHAT_ID, EXCLUDE_CHAT_ID),
        ) as cursor:
            row = await cursor.fetchone() or [0, 0, 0]
            videos_sin_thumb, videos_verticales, videos_largos = [r or 0 for r in row]

        dump_videos_select = (
            "SUM(CASE WHEN v.dump_message_id IS NOT NULL THEN 1 ELSE 0 END) AS dump_videos,"
            if has_dump_message_id
            else "0 AS dump_videos,"
        )
        blocked_select = (
            "SUM(CASE WHEN v.dump_fail = 1 AND v.dump_message_id IS NULL THEN 1 ELSE 0 END) AS blocked"
            if has_dump_fail and has_dump_message_id
            else "0 AS blocked"
        )

        # Consulta optimizada con CTE para top sin thumb
        if has_dump_message_id and has_dump_fail:
            # Versión completa con dump y blocked
            async with db.execute(
                """
                WITH sin_thumb_counts AS (
                    SELECT chat_id, COUNT(*) as sin_thumb_count
                    FROM videos_telegram 
                    WHERE has_thumb = 0 AND chat_id NOT IN (?, ?)
                    GROUP BY chat_id
                    HAVING sin_thumb_count > 0
                    ORDER BY sin_thumb_count DESC
                    LIMIT ?
                )
                SELECT 
                    stc.chat_id,
                    COALESCE(c.name, CAST(stc.chat_id AS TEXT)) AS name,
                    c.username,
                    stc.sin_thumb_count AS sin_thumb,
                    COALESCE(cv.videos_count, 0) AS total_videos,
                    COALESCE(dump_counts.dump_videos, 0) AS dump_videos,
                    COALESCE(blocked_counts.blocked, 0) AS blocked
                FROM sin_thumb_counts stc
                LEFT JOIN chats c ON c.chat_id = stc.chat_id
                LEFT JOIN chat_video_counts cv ON cv.chat_id = stc.chat_id
                LEFT JOIN (
                    SELECT chat_id, COUNT(*) as dump_videos
                    FROM videos_telegram 
                    WHERE dump_message_id IS NOT NULL 
                    GROUP BY chat_id
                ) dump_counts ON dump_counts.chat_id = stc.chat_id
                LEFT JOIN (
                    SELECT chat_id, COUNT(*) as blocked
                    FROM videos_telegram 
                    WHERE dump_fail = 1 AND dump_message_id IS NULL 
                    GROUP BY chat_id
                ) blocked_counts ON blocked_counts.chat_id = stc.chat_id
                ORDER BY stc.sin_thumb_count DESC
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, EXCLUDE_CHAT_ID, limit),
            ) as cursor:
                top_no_thumb_rows = await cursor.fetchall()
        else:
            # Versión simplificada
            async with db.execute(
                """
                WITH sin_thumb_counts AS (
                    SELECT chat_id, COUNT(*) as sin_thumb_count
                    FROM videos_telegram 
                    WHERE has_thumb = 0 AND chat_id NOT IN (?, ?)
                    GROUP BY chat_id
                    HAVING sin_thumb_count > 0
                    ORDER BY sin_thumb_count DESC
                    LIMIT ?
                )
                SELECT 
                    stc.chat_id,
                    COALESCE(c.name, CAST(stc.chat_id AS TEXT)) AS name,
                    c.username,
                    stc.sin_thumb_count AS sin_thumb,
                    COALESCE(cv.videos_count, 0) AS total_videos,
                    0 AS dump_videos,
                    0 AS blocked
                FROM sin_thumb_counts stc
                LEFT JOIN chats c ON c.chat_id = stc.chat_id
                LEFT JOIN chat_video_counts cv ON cv.chat_id = stc.chat_id
                ORDER BY stc.sin_thumb_count DESC
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, EXCLUDE_CHAT_ID, limit),
            ) as cursor:
                top_no_thumb_rows = await cursor.fetchall()

        async with db.execute(
            """
            SELECT cv.chat_id,
                   COALESCE(c.name, CAST(cv.chat_id AS TEXT)) AS name,
                   c.username,
                   cv.videos_count
            FROM chat_video_counts cv
            LEFT JOIN chats c ON c.chat_id = cv.chat_id
            WHERE cv.chat_id NOT IN (?, ?)
            ORDER BY cv.videos_count DESC
            LIMIT ?
            """,
            (CACHE_DUMP_VIDEOS_CHANNEL_ID, EXCLUDE_CHAT_ID, limit),
        ) as cursor:
            top_groups_rows = await cursor.fetchall()

        restricted_rows = []
        if has_dump_fail and has_dump_message_id:
            async with db.execute(
                f"""
                SELECT v.chat_id,
                       COALESCE(c.name, CAST(v.chat_id AS TEXT)) AS name,
                       c.username,
                       {blocked_select}
                FROM videos_telegram v
                LEFT JOIN chats c ON c.chat_id = v.chat_id
                WHERE v.chat_id NOT IN (?, ?)
                GROUP BY v.chat_id, c.username, c.name
                HAVING blocked > 0
                ORDER BY blocked DESC
                LIMIT ?
                """,
                (CACHE_DUMP_VIDEOS_CHANNEL_ID, EXCLUDE_CHAT_ID, limit),
            ) as cursor:
                restricted_rows = await cursor.fetchall()

    def build_link(chat_id: int) -> str:
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
        if len(r) >= 7:  # Versión completa con dump y blocked
            chat_id, name, username, sin_thumb, total_videos, dump_videos, blocked = r
        else:  # Versión simplificada
            chat_id, name, username, sin_thumb, total_videos = r[:5]
            dump_videos = 0
            blocked = 0
        
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
    log_timing(f"Endpoint /api/stats terminado")
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
