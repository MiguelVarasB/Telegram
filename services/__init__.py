"""
Módulo de servicios.
"""
"""
Módulo de servicios (Lógica de negocio y clientes externos).
"""
from .telegram_client import (
    start_client,
    stop_client,
    get_client,
    warmup_cache,
    with_reconnect,  # Ahora sí existe
    ensure_connected,
    reconnect_client
)
from .folder_sync import refresh_manual_folder_from_telegram
from .video_streamer import TelegramVideoSender
# --- NUEVO IMPORT ---
from .prefetch import prefetch_channel_videos_to_ram
from .thumb_worker_hibrido import background_thumb_downloader

__all__ = [
    "get_client",
    "start_client",
    "stop_client",
    "warmup_cache",
    "reconnect_client",
    "ensure_connected",
    "with_reconnect",
    "refresh_manual_folder_from_telegram",
    "TelegramVideoSender",
    "prefetch_channel_videos_to_ram", # <--- EXPORTADO PÚBLICAMENTE
    "background_thumb_downloader",
]