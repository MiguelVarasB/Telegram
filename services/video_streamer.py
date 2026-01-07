"""
Servicio de streaming híbrido TOTAL: Disco Cacheado -> RAM -> Telegram.
Versión CORREGIDA: Manejo de desconexiones (asyncio.CancelledError).
"""
import os
import asyncio
import aiofiles
from .telegram_client import ensure_connected, reconnect_client
from .memory_cache import get_from_ram, store_in_ram
from .disk_cache import get_cache_path, touch_file
from config import SMART_CACHE_ENABLED

class TelegramVideoSender:
    def __init__(self, client, chat_id: int, message_id: int, video_id: str = None, local_path: str = None):
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.video_id = video_id
        self.local_path = local_path
        
        self.message = None
        self.total_size = 0
        self.mime_type = "video/mp4"
        self.is_fully_cached = False
    
    async def setup(self, max_retries: int = 3):
        # 1. VIDEO COMPLETO (Descarga manual)
        if self.local_path and os.path.exists(self.local_path):
            self.total_size = os.path.getsize(self.local_path)
            self.mime_type = "video/mp4"
            print(f"🚀 [Stream] LOCAL COMPLETO: {self.local_path}")
            return

        # 2. SMART CACHE (Disco Parcial)
        # Revisamos si existe el archivo de caché generado automáticamente
        smart_path = get_cache_path(self.video_id) if self.video_id else None
        
        if SMART_CACHE_ENABLED and smart_path and os.path.exists(smart_path):
            touch_file(self.video_id) # Marcar como usado para que no se borre
            # Cargamos ese pedazo a RAM al vuelo para usarlo
            try:
                async with aiofiles.open(smart_path, mode='rb') as f:
                    data = await f.read()
                    # Guardamos en RAM temporalmente (sin metadatos aún, se actualizarán)
                    store_in_ram(self.video_id, data, 0, "", None)
            except Exception as e:
                print(f"⚠️ Error leyendo SmartCache: {e}")

        # 3. RAM CACHE
        if self.video_id:
            cached = get_from_ram(self.video_id)
            if cached:
                # Si tiene metadatos completos, usamos la RAM
                if cached.get('total_size', 0) > 0 and cached.get('message'):
                     self.total_size = cached['total_size']
                     self.mime_type = cached['mime_type']
                     self.message = cached['message']
                     
                     ram_size = len(cached['data'])
                     if ram_size >= self.total_size:
                         self.is_fully_cached = True
                         print(f"🧠 [Stream] Video 100% en CACHE. Telegram OFF.")
                     else:
                         print(f"🧠 [Stream] Cache HIT (RAM/Disk). Buffer: {ram_size/1024/1024:.1f} MB")
                     return

        # 4. TELEGRAM (Fallback)
        await self._fetch_from_telegram_with_retries(max_retries)

    async def _fetch_from_telegram_with_retries(self, max_retries):
        last_error = None
        for attempt in range(max_retries):
            try:
                await ensure_connected()
                self.message = await self.client.get_messages(self.chat_id, self.message_id)
                if self.message:
                    media = self.message.video or self.message.document
                    if not media:
                        raise ValueError(f"Mensaje {self.message_id} sin media (video/document)")

                    self.total_size = getattr(media, "file_size", 0) or 0
                    self.mime_type = getattr(media, "mime_type", "video/mp4") or "video/mp4"
                    
                    # ACTUALIZAR CACHE CON METADATOS
                    if self.video_id and get_from_ram(self.video_id):
                        cached = get_from_ram(self.video_id)
                        store_in_ram(self.video_id, cached['data'], self.total_size, self.mime_type, self.message)
                    
                    print(f" [Stream] Setup LENTO desde TELEGRAM (ID: {self.message_id})")
                    print(f"☁️ [Stream] Setup LENTO desde TELEGRAM (ID: {self.message_id})")
                    return
            except OSError as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    await reconnect_client()
            except Exception as e:
                raise e
        if last_error: raise last_error

    def get_headers(self, start: int, end: int) -> dict:
        return {
            "Content-Range": f"bytes {start}-{end}/{self.total_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Type": self.mime_type,
        }
    
    async def stream_generator(self, start: int, end: int):
        try:
            # CASO A: LOCAL COMPLETO
            if self.local_path and os.path.exists(self.local_path):
                chunk_size = 64 * 1024
                async with aiofiles.open(self.local_path, mode='rb') as f:
                    await f.seek(start)
                    bytes_to_send = end - start + 1
                    while bytes_to_send > 0:
                        read_size = min(chunk_size, bytes_to_send)
                        data = await f.read(read_size)
                        if not data: break
                        yield data
                        bytes_to_send -= len(data)
                return

            # CASO B: HÍBRIDO (RAM/Disk -> Telegram)
            bytes_sent = 0
            bytes_to_send = end - start + 1
            current_offset = start
            
            # 1. Intentar servir desde RAM (que contiene datos del disco)
            if self.video_id:
                cached = get_from_ram(self.video_id)
                if cached:
                    ram_data = cached['data']
                    ram_size = len(ram_data)
                    
                    if start < ram_size:
                        end_in_ram = min(start + bytes_to_send, ram_size)
                        chunk = ram_data[start:end_in_ram]
                        
                        yield chunk
                        sent_len = len(chunk)
                        bytes_sent += sent_len
                        current_offset += sent_len
                        
                        if bytes_sent >= bytes_to_send:
                            return

            if self.is_fully_cached: return 

            # 2. Transición a Telegram
            if not self.message:
                await self._fetch_from_telegram_with_retries(3)

            # Loop Telegram con manejo de cancelación
            TG_CHUNK_SIZE = 1024 * 1024
            chunk_index = current_offset // TG_CHUNK_SIZE
            offset_in_chunk = current_offset % TG_CHUNK_SIZE
            
            async for chunk in self.client.stream_media(self.message, offset=chunk_index):
                # Verificar conexión antes de procesar
                if not self.client.is_connected:
                    print("⚠️ Cliente de Telegram desconectado durante stream.")
                    break

                # Recorte inicial
                if offset_in_chunk > 0:
                    if len(chunk) <= offset_in_chunk:
                        offset_in_chunk -= len(chunk)
                        chunk_index += 1
                        continue
                    chunk = chunk[offset_in_chunk:]
                    offset_in_chunk = 0
                
                # Recorte final
                remaining = bytes_to_send - bytes_sent
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                
                if chunk:
                    yield chunk
                    bytes_sent += len(chunk)
                
                chunk_index += 1
                if bytes_sent >= bytes_to_send:
                    break

        except asyncio.CancelledError:
            # El cliente (navegador) suele cancelar el stream al cambiar de rango o cerrar el modal.
            # No lo tratamos como error para evitar 500 innecesarios.
            print(f"🛑 [Stream] Cliente canceló conexión (Video {self.video_id}).")
            return
        except Exception as e:
            print(f"❌ [Stream] Error inesperado: {e}")
            raise e