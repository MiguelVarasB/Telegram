"""
Servicio de Pre-carga optimizado con Smart Disk Cache y aiosqlite.
Descarga 5MB por video y deja que el DiskManager administre el espacio.
"""
import asyncio
import aiosqlite 
import os
import aiofiles
from .telegram_client import get_client
from .memory_cache import store_in_ram, clear_ram_cache
from .disk_cache import save_to_disk_smart, get_cache_path, touch_file
from config import DB_PATH, TARGET_VIDEO_CACHE_SIZE, SMART_CACHE_ENABLED

CONCURRENT_LIMIT = 4 

async def prefetch_channel_videos_to_ram(chat_id: int):
    if not SMART_CACHE_ENABLED:
        return
    # Limpiamos RAM (La RAM es volátil, el Disco es persistente)
    clear_ram_cache()

    # --- Consulta DB 100% Asíncrona ---
    video_list = []
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT message_id, id FROM videos_telegram WHERE chat_id = ?", (chat_id,)) as cursor:
                rows = await cursor.fetchall()
                video_list = [{'msg_id': r[0], 'vid_id': r[1]} for r in rows]
    except Exception as e:
        print(f"⚠️ Error leyendo DB en prefetch: {e}")
        return
    # ------------------------------------------

    if not video_list: return

    print(f"🚀 [SmartCache] Verificando {len(video_list)} videos. Meta: {TARGET_VIDEO_CACHE_SIZE/1024/1024} MB c/u.")
    
    client = get_client()
    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    
    async def load_one_video(vid_data):
        async with sem:
            msg_id = vid_data['msg_id']
            vid_id = vid_data['vid_id']
            disk_path = get_cache_path(vid_id)
            
            try:
                # A. OBTENER METADATOS
                # Usamos get_messages (Pyrogram cachea esto internamente si ya se vio)
                msg = await client.get_messages(chat_id, msg_id)
                if not msg: return False

                media = msg.video or msg.document
                if not media: return False
                
                total_size = media.file_size
                mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
                
                # B. VERIFICAR SI YA ESTÁ EN DISCO (CACHE HIT)
                if os.path.exists(disk_path) and os.path.getsize(disk_path) >= TARGET_VIDEO_CACHE_SIZE:
                    # Le hacemos "touch" para marcarlo como reciente
                    touch_file(vid_id)
                    
                    # Lo cargamos a RAM para acceso ultra-rápido
                    async with aiofiles.open(disk_path, mode='rb') as f:
                        data = await f.read(TARGET_VIDEO_CACHE_SIZE)
                        store_in_ram(vid_id, data, total_size, mime_type, message_obj=msg)
                    return True

                # C. MISS -> DESCARGAR DE TELEGRAM
                buffer = bytearray()
                chunks_needed = (TARGET_VIDEO_CACHE_SIZE // (1024*1024)) + 1
                
                async for chunk in client.stream_media(msg, limit=chunks_needed):
                    buffer.extend(chunk)
                    if len(buffer) >= TARGET_VIDEO_CACHE_SIZE:
                        break 
                
                # Guardamos en DISCO (El gestor borrará otros antiguos si hace falta espacio)
                await save_to_disk_smart(vid_id, bytes(buffer))
                
                # Y también en RAM para esta sesión
                store_in_ram(vid_id, bytes(buffer), total_size, mime_type, message_obj=msg)
                
                return True
            except asyncio.CancelledError:
                return False
            except Exception as e:
                print(f"❌ [Err] {vid_id}: {e}")
                return False

    # Ejecutar cargas concurrentemente
    tasks = [load_one_video(v) for v in video_list]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        return
    print(f"🏁 [SmartCache] Proceso terminado para {chat_id}.")