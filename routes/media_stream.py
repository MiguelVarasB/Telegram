import os
import re
import time
import asyncio
import aiofiles
from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse

from config import DUMP_FOLDER, DB_PATH, SMART_CACHE_ENABLED, MQTT_ENABLED
from services import get_client, TelegramVideoSender, prefetch_channel_videos_to_ram, background_thumb_downloader
from utils import save_image_as_webp
from utils.mqtt_manager import get_mqtt_manager
from .media_common import get_video_info_from_db

from fastapi import BackgroundTasks
import aiosqlite

router = APIRouter()
downloads_status: dict[str, dict] = {}
download_cancel_flags: dict[str, asyncio.Event] = {}


def _sanitize_filename(name: str) -> str:
    if not name:
        return "video"
    cleaned = re.sub(r"[\\/:*?\"<>|]+", "-", name).strip().strip(".")
    return cleaned or "video"


def _build_download_path(chat_id: int, video_id: str, video_name: str | None = None) -> str:
    save_dir = os.path.join(DUMP_FOLDER, "descarga", str(chat_id))
    os.makedirs(save_dir, exist_ok=True)

    base_name = _sanitize_filename(video_name or "")
    root, ext = os.path.splitext(base_name)
    if not root:
        root = video_id
    if not ext:
        ext = ".mp4"

    filename = f"{video_id}-{root}{ext}"
    return os.path.join(save_dir, filename)


# --- BACKGROUND TASKS ---
async def _background_video_download(chat_id: int, message_id: int, video_id: str, video_name: str | None = None):
    """
    Descarga el video COMPLETO al disco SSD en segundo plano.
    """
    client = get_client()
    file_path = _build_download_path(chat_id, video_id, video_name)
    filename = os.path.basename(file_path)
    download_id = f"{chat_id}:{message_id}:{video_id}"
    cancel_flag = download_cancel_flags.setdefault(download_id, asyncio.Event())

    print(f"⬇️ [BG] Iniciando descarga completa a DISCO: {filename}")

    try:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"ℹ️ [BG] Archivo ya existente: {file_path}")
            downloads_status[download_id] = {
                "status": "completed",
                "chat_id": chat_id,
                "message_id": message_id,
                "video_id": video_id,
                "file_path": file_path,
                "filename": filename,
                "current": os.path.getsize(file_path),
                "total": os.path.getsize(file_path),
                "speed": 0,
                "eta": 0,
                "started_at": time.time(),
                "updated_at": time.time(),
            }
            return
        # Necesitamos el mensaje completo para descargar el media (message_id solo causa error de int sin file_id)
        msg = await client.get_messages(chat_id, message_id)
        media = getattr(msg, "video", None) or getattr(msg, "document", None)
        if not media:
            raise ValueError("Mensaje sin media descargable")

        downloads_status[download_id] = {
            "status": "downloading",
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "file_path": file_path,
            "filename": filename,
            "current": 0,
            "total": getattr(media, "file_size", 0) or 0,
            "speed": 0,
            "eta": None,
            "started_at": time.time(),
            "updated_at": time.time(),
        }

        def _progress(current: int, total: int, *args):
            if cancel_flag.is_set():
                raise asyncio.CancelledError("cancelled by user")
            now = time.time()
            elapsed = max(now - downloads_status[download_id]["started_at"], 1e-3)
            speed = current / elapsed
            eta = max(int((total - current) / speed), 0) if total else None
            downloads_status[download_id].update(
                {
                    "current": current,
                    "total": total,
                    "speed": speed,
                    "eta": eta,
                    "updated_at": now,
                }
            )
            
            if MQTT_ENABLED:
                mqtt_mgr = get_mqtt_manager()
                if mqtt_mgr and mqtt_mgr.is_connected():
                    mqtt_mgr.publish_download_progress(
                        chat_id=chat_id,
                        message_id=message_id,
                        video_id=video_id,
                        status="downloading",
                        current=current,
                        total=total,
                        speed=speed,
                        eta=eta,
                    )

        downloaded = await client.download_media(
            media, file_name=file_path, progress=_progress
        )

        if downloaded:
            print(f"✅ [BG] Descarga en disco completada: {file_path}")
            now = time.time()
            size_now = os.path.getsize(file_path) if os.path.exists(file_path) else downloads_status[download_id]["current"]
            downloads_status[download_id].update(
                {
                    "status": "completed",
                    "current": size_now,
                    "total": size_now,
                    "speed": 0,
                    "eta": 0,
                    "updated_at": now,
                    "finished_at": now,
                }
            )
            
            if MQTT_ENABLED:
                mqtt_mgr = get_mqtt_manager()
                if mqtt_mgr and mqtt_mgr.is_connected():
                    mqtt_mgr.publish_download_progress(
                        chat_id=chat_id,
                        message_id=message_id,
                        video_id=video_id,
                        status="completed",
                        current=size_now,
                        total=size_now,
                        speed=0,
                        eta=0,
                    )
            # Usamos aiosqlite para la actualización final
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE videos_telegram SET ruta_local = ?, en_mega = 0 WHERE chat_id = ? AND message_id = ?",
                    (file_path, chat_id, message_id),
                )
                await db.commit()
    except asyncio.CancelledError:
        print(f"🛑 [BG] Descarga cancelada {video_id}")
        downloads_status[download_id] = {
            "status": "cancelled",
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "file_path": file_path,
            "filename": filename,
            "current": downloads_status.get(download_id, {}).get("current", 0),
            "total": downloads_status.get(download_id, {}).get("total", 0),
            "speed": 0,
            "eta": None,
            "started_at": downloads_status.get(download_id, {}).get("started_at", time.time()),
            "updated_at": time.time(),
        }
        # limpiar archivo parcial si existe
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
    except Exception as e:
        print(f"❌ [BG] Error descarga {video_id}: {e}")
        downloads_status[download_id] = {
            "status": "failed",
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "file_path": file_path,
            "filename": filename,
            "error": str(e),
            "current": downloads_status.get(download_id, {}).get("current", 0),
            "total": downloads_status.get(download_id, {}).get("total", 0),
            "speed": 0,
            "eta": None,
            "started_at": downloads_status.get(download_id, {}).get("started_at", time.time()),
            "updated_at": time.time(),
        }
    finally:
        download_cancel_flags.pop(download_id, None)


@router.get("/video_stream/{chat_id}/{message_id}")
async def video_stream(chat_id: int, message_id: int, file_unique_id: str | None = Query(None), range: str = Header(None)):
    """
    Streaming Híbrido: Disco -> RAM -> Telegram.
    """
    client = get_client()

    # DB call async
    video_id, local_path = await get_video_info_from_db(chat_id, message_id, file_unique_id)

    # Si está descargado en disco, servimos directamente el archivo local
    if local_path and os.path.exists(local_path):
        total_size = os.path.getsize(local_path)
        start, end = 0, total_size - 1
        if range:
            try:
                r = range.replace("bytes=", "").split("-")
                start = int(r[0])
                if len(r) > 1 and r[1]:
                    end = int(r[1])
            except Exception:
                pass

        if start >= total_size:
            raise HTTPException(status_code=416, detail="Range not satisfiable")

        async def local_stream(start_pos: int, end_pos: int):
            chunk_size = 64 * 1024
            bytes_to_send = end_pos - start_pos + 1
            async with aiofiles.open(local_path, mode='rb') as f:
                await f.seek(start_pos)
                while bytes_to_send > 0:
                    read_size = min(chunk_size, bytes_to_send)
                    data = await f.read(read_size)
                    if not data:
                        break
                    yield data
                    bytes_to_send -= len(data)

        mime_type = "video/mp4"
        headers = {
            "Content-Range": f"bytes {start}-{end}/{total_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": mime_type,
        }
        return StreamingResponse(
            local_stream(start, end),
            status_code=206,
            headers=headers,
            media_type=mime_type,
        )

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
async def download_video(chat_id: int, message_id: int, video_id: str, video_name: str | None = None, background_tasks: BackgroundTasks = None):
    """Botón 'Descargar' (Disco Completo)."""
    file_path = _build_download_path(chat_id, video_id, video_name)
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        return {"status": "ready", "message": "Archivo ya disponible", "path": file_path}
    if background_tasks is not None:
        background_tasks.add_task(_background_video_download, chat_id, message_id, video_id, video_name)
    return {"status": "started", "message": "Descarga completa iniciada en 2do plano", "path": file_path}


@router.get("/api/download/file/{chat_id}/{video_id}")
async def serve_download(chat_id: int, video_id: str):
    """Entrega el archivo descargado si existe."""
    target_dir = os.path.join(DUMP_FOLDER, "descarga", str(chat_id))
    if not os.path.isdir(target_dir):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    candidates = [f for f in os.listdir(target_dir) if f.startswith(f"{video_id}-")]
    if not candidates:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    file_path = os.path.join(target_dir, candidates[0])
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(file_path, media_type="application/octet-stream", filename=os.path.basename(file_path))


@router.get("/api/downloads/status")
async def get_downloads_status():
    """
    Devuelve estado de descargas en curso/completadas en memoria.
    Incluye velocidad y ETA cuando está descargando.
    """
    # Copia superficial para evitar mutaciones mientras serializamos
    payload = []
    for d_id, info in downloads_status.items():
        data = info.copy()
        # Normalizamos valores numéricos
        for key in ("current", "total", "speed", "eta"):
            val = data.get(key)
            if val is None:
                continue
            try:
                data[key] = float(val)
            except Exception:
                pass
        payload.append(data)

    # Orden: descargas activas primero, luego por updated_at desc
    def sort_key(item: dict):
        status = item.get("status", "")
        is_active = 0 if status == "downloading" else 1
        return (is_active, -1 * item.get("updated_at", 0))

    payload.sort(key=sort_key)
    return JSONResponse(payload)


@router.post("/api/downloads/cancel/{chat_id}/{message_id}/{video_id}")
async def cancel_download(chat_id: int, message_id: int, video_id: str):
    download_id = f"{chat_id}:{message_id}:{video_id}"
    flag = download_cancel_flags.get(download_id)
    if flag:
        flag.set()
        downloads_status[download_id] = {
            **downloads_status.get(download_id, {}),
            "status": "cancelled",
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "updated_at": time.time(),
        }
        return {"status": "cancelled", "message": "Descarga cancelada"}
    if download_id in downloads_status:
        downloads_status[download_id]["status"] = "cancelled"
        downloads_status[download_id]["updated_at"] = time.time()
        return {"status": "cancelled", "message": "Descarga marcada como cancelada"}
    raise HTTPException(status_code=404, detail="Descarga no encontrada")


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
