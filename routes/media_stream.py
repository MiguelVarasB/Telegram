import os
import asyncio
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse

from config import DUMP_FOLDER, DB_PATH, SMART_CACHE_ENABLED
from services import get_client, TelegramVideoSender, prefetch_channel_videos_to_ram, background_thumb_downloader
from utils import save_image_as_webp
from .media_common import get_video_info_from_db

from fastapi import BackgroundTasks
import aiosqlite

router = APIRouter()


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
                    (file_path, chat_id, message_id),
                )
                await db.commit()
    except Exception as e:
        print(f"❌ [BG] Error descarga {video_id}: {e}")


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
