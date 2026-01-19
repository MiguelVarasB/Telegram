"""
Cliente de Telegram (Pyrogram) singleton con reconexión automática.
"""
import asyncio
import os
import shutil
from typing import Optional

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
from utils import log_timing

# Diccionario para cachear instancias de clientes
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
        return clone_path

    shutil.copy2(base_file, clone_file)
    for suffix in ("-wal", "-shm"):
        src = f"{base_file}{suffix}"
        dst = f"{clone_file}{suffix}"
        if os.path.exists(src):
            shutil.copy2(src, dst)

    return clone_path


def _get_or_create_client(session_name: str, session_path: str) -> Client:
    """Obtiene un cliente cacheado o crea uno nuevo."""
    if session_name not in _clients:
        _clients[session_name] = Client(
            session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            # CRÍTICO: no_updates=True hace que el cliente sea 'pasivo' y arranque más rápido
            no_updates=True, 
        )
    return _clients[session_name]


def get_client(
    use_server_session: bool = False,
    clone_for_cli: bool = False,
    custom_session_name: Optional[str] = None,
) -> Client:
    """Retorna un cliente de Pyrogram cacheado por session_name.

    Si se pasa custom_session_name, se utilizará ese nombre y ruta.
    """
    os.makedirs(FOLDER_SESSIONS, exist_ok=True)

    if custom_session_name:
        session_name = custom_session_name
        session_path = os.path.join(FOLDER_SESSIONS, session_name)
    elif clone_for_cli:
        session_name = SESSION_NAME_CLI
        session_path = os.path.join(FOLDER_SESSIONS, session_name)
    else:
        session_name = SESSION_NAME_SERVER if use_server_session else SESSION_NAME
        session_path = os.path.join(FOLDER_SESSIONS, session_name)

    return _get_or_create_client(session_name, session_path)


def get_unigram_client() -> Client:
    """Atajo para obtener un cliente usando la sesión "Unigram"."""
    return get_client(custom_session_name="Unigram")


async def start_client(use_server_session: bool = False):
    """Inicia el cliente de Telegram."""
    client = get_client(use_server_session=use_server_session)
    if not client.is_connected:
        await client.start()
        log_timing("🚀 Cliente de Telegram iniciado (conectado)")
    else:
        log_timing("🚀 Cliente de Telegram ya estaba conectado")


async def stop_client():
    """Detiene el cliente de Telegram."""
    client = get_client()
    if client.is_connected:
        await client.stop()
    log_timing("🛑 Cliente de Telegram detenido")


async def reconnect_client():
    """Reconecta el cliente de Telegram de forma segura."""
    async with _reconnect_lock:
        client = get_client()
        try:
            if client.is_connected:
                await client.stop()
        except Exception as e:
            log_timing(f"⚠️ Error al detener cliente: {e}")
        
        try:
            log_timing("🔄 Reconectando cliente de Telegram...")
            await client.start()
            log_timing("✅ Cliente reconectado exitosamente")
            return True
        except (AuthKeyUnregistered, SessionRevoked) as e:
            log_timing(f"❌ Error de sesión: {e}. Necesitas volver a autenticarte.")
            return False
        except Exception as e:
            log_timing(f"❌ Error al reconectar: {e}")
            return False


async def ensure_connected():
    """Verifica que el cliente esté conectado."""
    client = get_client()
    if not client.is_connected:
        return await reconnect_client()
    return True


async def with_reconnect(coro, *args, **kwargs):
    """
    Ejecuta una corrutina asegurando que el cliente esté conectado.
    Si falla, intenta reconectar y reintentar una vez.
    """
    try:
        if not await ensure_connected():
            raise ConnectionError("No se pudo conectar a Telegram")
        return await coro(*args, **kwargs)
    except Exception as first_error:
        log_timing(f"⚠️ Error en operación Telegram: {first_error}. Intentando reconexión...")
        if await reconnect_client():
            return await coro(*args, **kwargs)
        raise first_error


async def warmup_cache(limit: int = 100):
    """
    Descarga diálogos para 'calentar' la caché interna de Pyrogram.
    Esto ayuda a resolver PeerIds rápidamente después.
    """
    client = get_client()
    log_timing("⏳ (Background) Sincronizando lista de chats...")
    try:
        count = 0
        async for dialog in client.get_dialogs(limit=limit):
            count += 1
        log_timing(f"✅ (Background) Sincronización completada ({count} chats)")
    except Exception as e:
        log_timing(f"⚠️ Aviso: Sincronización parcial ({e})")