"""
Módulo de base de datos.
Expone las funciones principales para uso externo.
"""
from .connection import (
    init_db,
    get_db,
    transaction,
    get_sync_connection,
    DatabaseConnectionError
)

# 1. Funciones de CHATS (Aquí vive db_add_chat_folder)
from .chats import (
    db_upsert_chat_basic,
    db_upsert_chat_from_ci,
    db_get_chat,
    db_get_chat_folders,
    db_add_chat_folder, # <--- Correcto: Viene de chats.py
    db_upsert_chat_video_count,
    db_get_chat_scan_meta,
    db_bulk_upsert_chats
)

# 2. Funciones de VIDEOS
from .videos import (
    db_upsert_video,
    db_add_video_file_id,
    db_upsert_video_message,
    db_get_video_messages,
    db_count_videos_by_chat,
    db_bulk_upsert_videos,
    db_bulk_upsert_video_messages,
    db_bulk_add_video_file_ids
)

# 3. Funciones de CARPETAS (Solo get_folder_items_from_db está aquí)
from .folders import (
    get_folder_items_from_db,
    get_all_chats_with_counts
)

# 4. Funciones de TAGS
from .tags import (
    db_upsert_tag,
    db_list_tags,
    db_ensure_tags_table,
)

# 5. Funciones NUEVAS de OPTIMIZACIÓN (Queries y Counters)
from .queries import db_get_channel_videos, db_get_chat_info
from .counters import update_chat_stats_background

__all__ = [
    # Conexión
    "init_db", "get_db", "transaction", "get_sync_connection", "DatabaseConnectionError",
    
    # Chats
    "db_upsert_chat_basic", "db_upsert_chat_from_ci", "db_get_chat",
    "db_get_chat_folders", "db_add_chat_folder", "db_upsert_chat_video_count",
    "db_get_chat_scan_meta", "db_bulk_upsert_chats",
    
    # Videos
    "db_upsert_video", "db_add_video_file_id", "db_upsert_video_message",
    "db_get_video_messages", "db_count_videos_by_chat",
    "db_bulk_upsert_videos", "db_bulk_upsert_video_messages", "db_bulk_add_video_file_ids",
    
    # Folders
    "get_folder_items_from_db", "get_all_chats_with_counts",
    
    # Tags
    "db_upsert_tag", "db_list_tags", "db_ensure_tags_table",

    # Optimization
    "db_get_channel_videos", "db_get_chat_info", "update_chat_stats_background"
]