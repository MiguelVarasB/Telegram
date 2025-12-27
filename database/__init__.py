"""
Módulo de base de datos.
Expone las funciones principales para uso externo.
"""
from .connection import init_db, get_connection
from .chats import (
    db_upsert_chat_basic,
    db_upsert_chat_from_ci,
    db_get_chat,
    db_get_chat_folders,
    db_add_chat_folder,
    db_upsert_chat_video_count,
)
from .videos import db_upsert_video, db_add_video_file_id, db_upsert_video_message, db_get_video_messages, db_count_videos_by_chat
from .folders import get_folder_items_from_db

__all__ = [
    "init_db",
    "get_connection",
    "db_upsert_chat_basic",
    "db_upsert_chat_from_ci",
    "db_get_chat",
    "db_get_chat_folders",
    "db_add_chat_folder",
    "db_upsert_chat_video_count",
    "db_upsert_video",
    "db_add_video_file_id",
    "db_upsert_video_message",
    "db_get_video_messages",
    "db_count_videos_by_chat",
    "get_folder_items_from_db",
]
