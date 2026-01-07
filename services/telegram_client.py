"""
Cliente de Telegram (Pyrogram) singleton con reconexión automática.
"""
import asyncio
import os
import shutil
from pyrogram import Client
from pyrogram.errors import AuthKeyUnregistered, SessionRevoked
from config import (
    API_ID,
    API_HASH,
    SESSION_NAME,
    SESSION_NAME_CLI,
    SESSION_NAME_SERVER,
    FOLDER_SESSIONS,
)

# Cliente en modo pasivo (no_updates=True) para evitar errores de PeerInvalid
# Permitimos múltiples instancias (p. ej., un clon para CLI) cacheadas por ruta.
_clients: dict[str, Client] = {}
_reconnect_lock = asyncio.Lock()


def _copy_session_files(base_name: str, clone_name: str):
    """Clona los archivos .session para poder abrir la misma cuenta sin lock."""
    os.makedirs(FOLDER_SESSIONS, exist_ok=True)
    base_path = os.path.join(FOLDER_SESSIONS, base_name)
    clone_path = os.path.join(FOLDER_SESSIONS, clone_name)

    base_file = f"{base_path}.session"
    clone_file = f"{clone_path}.session"

    if not os.path.exists(base_file):
        return clone_path  # Nada que clonar (primera vez, se creará nuevo)

    shutil.copy2(base_file, clone_file)
    # Copiamos archivos WAL/SHM si existen para consistencia
    for suffix in ("-wal", "-shm"):
        src = f"{base_file}{suffix}"
        dst = f"{clone_file}{suffix}"
        if os.path.exists(src):
            shutil.copy2(src, dst)

    return clone_path


def _get_or_create_client(session_name: str, session_path: str) -> Client:
    """Obtiene un cliente cacheado o crea uno nuevo para el session_name dado."""
    if session_name not in _clients:
        _clients[session_name] = Client(
            session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            no_updates=True,
        )
    return _clients[session_name]


def get_client(use_server_session: bool = False, clone_for_cli: bool = False) -> Client:
    """Retorna un cliente de Pyrogram cacheado por session_name.

    use_server_session=True -> usa SESSION_NAME_SERVER (server).
    clone_for_cli=True -> clona archivos .session de la sesión principal a un nombre derivado
                          para evitar locks cuando el server ya está arriba.
    """
    os.makedirs(FOLDER_SESSIONS, exist_ok=True)

    if clone_for_cli:
        # Usa una sesión dedicada para CLI para evitar locks con el server
        session_name = SESSION_NAME_CLI
        session_path = os.path.join(FOLDER_SESSIONS, session_name)
    else:
        session_name = SESSION_NAME_SERVER if use_server_session else SESSION_NAME
        session_path = os.path.join(FOLDER_SESSIONS, session_name)

    return _get_or_create_client(session_name, session_path)


async def start_client(use_server_session: bool = False):
    """Inicia el cliente de Telegram.

    use_server_session=True => usa SESSION_NAME_SERVER para no compartir el .session con CLI.
    """
    client = get_client(use_server_session=use_server_session)
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

