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
from .chats import (
    db_upsert_chat_basic,
    db_upsert_chat_from_ci,
    db_get_chat,
    db_get_chat_folders,
    db_add_chat_folder,
    db_upsert_chat_video_count,
    db_get_chat_scan_meta,
)
from .videos import (
    db_upsert_video,
    db_add_video_file_id,
    db_upsert_video_message,
    db_get_video_messages,
    db_count_videos_by_chat
)
from .folders import get_folder_items_from_db
from .tags import (
    db_upsert_tag,
    db_list_tags,
    db_ensure_tags_table,
)

__all__ = [
    # Conexión
    "init_db",
    "get_db",
    "transaction",
    "get_sync_connection",
    "DatabaseConnectionError",
    
    # Chats
    "db_upsert_chat_basic",
    "db_upsert_chat_from_ci",
    "db_get_chat",
    "db_get_chat_folders",
    "db_add_chat_folder",
    "db_upsert_chat_video_count",
    "db_get_chat_scan_meta",
    "db_upsert_video",
    "db_add_video_file_id",
    "db_upsert_video_message",
    "db_get_video_messages",
    "db_count_videos_by_chat",
    "get_folder_items_from_db",
    
    # Tags
    "db_upsert_tag",
    "db_list_tags",
    "db_ensure_tags_table",
]
