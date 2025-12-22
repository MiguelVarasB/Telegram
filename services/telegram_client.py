"""
Cliente de Telegram (Pyrogram) singleton con reconexión automática.
"""
import asyncio
import os
from pyrogram import Client
from pyrogram.errors import AuthKeyUnregistered, SessionRevoked
from config import API_ID, API_HASH, SESSION_NAME, FOLDER_SESSIONS

# Cliente en modo pasivo (no_updates=True) para evitar errores de PeerInvalid
_client: Client | None = None
_reconnect_lock = asyncio.Lock()


def get_client() -> Client:
    """Retorna el cliente singleton de Pyrogram."""
    global _client
    if _client is None:
        os.makedirs(FOLDER_SESSIONS, exist_ok=True)
        session_path = os.path.join(FOLDER_SESSIONS, SESSION_NAME)
        _client = Client(session_path, api_id=API_ID, api_hash=API_HASH, no_updates=True)
    return _client


async def start_client():
    """Inicia el cliente de Telegram."""
    client = get_client()
    await client.start()
    print("🚀 Cliente de Telegram iniciado")


async def stop_client():
    """Detiene el cliente de Telegram."""
    client = get_client()
    if client.is_connected:
        await client.stop()
    print("🛑 Cliente de Telegram detenido")


async def reconnect_client():
    """Reconecta el cliente de Telegram de forma segura."""
    global _client
    async with _reconnect_lock:
        client = get_client()
        try:
            # Intentar detener si está conectado
            if client.is_connected:
                print("🔄 Desconectando cliente existente...")
                await client.stop()
        except Exception as e:
            print(f"⚠️ Error al detener cliente: {e}")
        
        # Reconectar
        try:
            print("🔄 Reconectando cliente de Telegram...")
            await client.start()
            print("✅ Cliente reconectado exitosamente")
            return True
        except (AuthKeyUnregistered, SessionRevoked) as e:
            print(f"❌ Error de sesión: {e}. Necesitas volver a autenticarte.")
            return False
        except Exception as e:
            print(f"❌ Error al reconectar: {e}")
            return False


async def ensure_connected():
    """Verifica que el cliente esté conectado, reconecta si es necesario."""
    client = get_client()
    if not client.is_connected:
        print("⚠️ Cliente desconectado, intentando reconectar...")
        return await reconnect_client()
    return True


async def with_reconnect(coro_func, *args, max_retries: int = 3, **kwargs):
    """
    Ejecuta una coroutine con reintentos y reconexión automática.
    
    Uso:
        result = await with_reconnect(client.get_messages, chat_id, message_id)
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Asegurar conexión antes de cada intento
            if not await ensure_connected():
                raise ConnectionError("No se pudo reconectar al cliente de Telegram")
            
            # Ejecutar la operación
            return await coro_func(*args, **kwargs)
            
        except OSError as e:
            # Errores de socket (WinError 10053, 10054, etc.)
            last_error = e
            print(f"⚠️ Error de conexión (intento {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Esperar antes de reintentar
                await reconnect_client()
            
        except Exception as e:
            # Otros errores, propagar directamente
            raise e
    
    # Si llegamos aquí, agotamos los reintentos
    raise last_error or ConnectionError("Error de conexión después de múltiples reintentos")


async def warmup_cache(limit: int = 100):
    """Descarga diálogos para 'calentar' la caché y obtener llaves de acceso."""
    client = get_client()
    print("⏳ Sincronizando lista de chats...")
    try:
        count = 0
        async for dialog in client.get_dialogs(limit=limit):
            count += 1
        print(f"✅ Sincronización completada ({count} chats)")
    except Exception as e:
        print(f"⚠️ Aviso: Sincronización parcial ({e})")

