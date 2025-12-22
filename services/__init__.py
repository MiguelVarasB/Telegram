"""
Módulo de servicios.
"""
from .telegram_client import (
    get_client, 
    start_client, 
    stop_client, 
    warmup_cache,
    reconnect_client,
    ensure_connected,
    with_reconnect,
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