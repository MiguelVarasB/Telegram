"""
Operaciones de base de datos relacionadas con chats.
Versión Async TOTAL (aiosqlite).
"""
import json
import datetime
import aiosqlite
from config import DB_PATH
from database.connection import get_db

async def db_upsert_chat_basic(
    chat_id: int,
    name: str | None,
    chat_type: str | None,
    username: str | None,
    is_owner: bool | int = False,
    is_public: bool | int = False,
    has_protected_content: bool | int = False,
    activo: bool | int = False,
    raw_json: str | None = None,
    last_message_date: str | None = None,
    ultimo_escaneo: str | None = None,
) -> None:
    """Inserta/actualiza un chat con los campos mínimos (Asíncrono)."""
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO chats (
                chat_id, name, type, photo_id, username,
                raw_json, last_message_date, ultimo_escaneo, updated_at,
                is_owner, is_public, has_protected_content, activo
            )
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                username=excluded.username,
                raw_json=excluded.raw_json,
                is_owner=excluded.is_owner,
                is_public=excluded.is_public,
                has_protected_content=excluded.has_protected_content,
                activo=excluded.activo,
                last_message_date=COALESCE(excluded.last_message_date, chats.last_message_date),
                ultimo_escaneo=COALESCE(excluded.ultimo_escaneo, chats.ultimo_escaneo),
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                name or "Sin Nombre",
                chat_type,
                username,
                raw_json,
                last_message_date,
                ultimo_escaneo,
                datetime.datetime.utcnow().isoformat(),
                1 if is_owner else 0,
                1 if is_public else 0,
                1 if has_protected_content else 0,
                1 if activo else 0,
            ),
        )
        await db.commit()


async def db_upsert_chat_from_ci(ci, last_message_date: str | None = None, activo: bool | int = False):
    """Guarda/actualiza un chat resuelto en la base de datos (Asíncrono)."""
    chat_id = ci.id
    name = ci.title or getattr(ci, "first_name", None) or "Sin Nombre"
    chat_type = str(ci.type).replace("ChatType.", "") if getattr(ci, "type", None) else None
    photo_id = ci.photo.small_file_id if getattr(ci, "photo", None) else None
    username = getattr(ci, "username", None)
    is_owner = getattr(ci, "is_creator", False) or getattr(ci, "is_self", False)
    is_public = bool(username)
    has_protected_content = getattr(ci, "has_protected_content", False)
    
    try:
        raw_dict = json.loads(str(ci))
        raw_json = json.dumps(raw_dict, ensure_ascii=False)
    except Exception:
        raw_json = str(ci)

    async with get_db() as db:
        await db.execute("""
            INSERT INTO chats (
                chat_id, name, type, photo_id, username,
                raw_json, last_message_date, ultimo_escaneo, updated_at,
                is_owner, is_public, has_protected_content, activo
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                photo_id=excluded.photo_id,
                username=excluded.username,
                raw_json=excluded.raw_json,
                is_owner=excluded.is_owner,
                is_public=excluded.is_public,
                has_protected_content=excluded.has_protected_content,
                activo=excluded.activo,
                last_message_date=COALESCE(excluded.last_message_date, chats.last_message_date),
                ultimo_escaneo=COALESCE(excluded.ultimo_escaneo, chats.ultimo_escaneo),
                updated_at=excluded.updated_at
        """, (
            chat_id,
            name,
            chat_type,
            photo_id,
            username,
            raw_json,
            last_message_date,
            None,
            datetime.datetime.utcnow().isoformat(),
            1 if is_owner else 0,
            1 if is_public else 0,
            1 if has_protected_content else 0,
            1 if activo else 0,
        ))
        await db.commit()


async def db_bulk_upsert_chats(chats_data: list[tuple]) -> None:
    """Inserta/actualiza chats en bloque para mayor rendimiento."""
    if not chats_data:
        return
    async with get_db() as db:
        await db.executemany(
            """
            INSERT INTO chats (
                chat_id, name, type, photo_id, username,
                raw_json, last_message_date, ultimo_escaneo, updated_at,
                is_owner, is_public, has_protected_content, activo
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                photo_id=excluded.photo_id,
                username=excluded.username,
                raw_json=excluded.raw_json,
                is_owner=excluded.is_owner,
                is_public=excluded.is_public,
                has_protected_content=excluded.has_protected_content,
                activo=excluded.activo,
                last_message_date=COALESCE(excluded.last_message_date, chats.last_message_date),
                ultimo_escaneo=COALESCE(excluded.ultimo_escaneo, chats.ultimo_escaneo),
                updated_at=excluded.updated_at
            """,
            chats_data,
        )
        await db.commit()


async def db_get_chat(chat_id: int) -> dict | None:
    """Obtiene un chat guardado por ID de forma ASÍNCRONA."""
    async with get_db() as db:
        async with db.execute(
            """
            SELECT
                chat_id, name, type, photo_id, username,
                raw_json, last_message_date, is_owner,
                is_public, has_protected_content
            FROM chats
            WHERE chat_id = ?
            """,
            (chat_id,),
        ) as cursor:
            row = await cursor.fetchone()
            
            if not row:
                return None
            
            return {
                "chat_id": row[0],
                "name": row[1],
                "type": row[2],
                "photo_id": row[3],
                "username": row[4],
                "raw_json": row[5],
                "last_message_date": row[6],
                "is_owner": row[7],
                "is_public": row[8],
                "has_protected_content": row[9],
            }


async def db_upsert_chat_video_count(
    chat_id: int,
    videos_count: int,
    scanned_at: str | None = None,
    duplicados: int | None = None,
    indexados: int | None = None,
) -> None:
    """Inserta/actualiza el conteo de videos por chat (incluye duplicados e indexados)."""
    scanned_at = scanned_at or datetime.datetime.utcnow().isoformat()
    # Si no se pasa indexados, asumimos videos_count como fallback (compatibilidad)
    idx_value = indexados if indexados is not None else videos_count
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO chat_video_counts (chat_id, videos_count, scanned_at, duplicados, indexados)
            VALUES (?, ?, ?, COALESCE(?, 0), COALESCE(?, 0))
            ON CONFLICT(chat_id) DO UPDATE SET
                videos_count = excluded.videos_count,
                scanned_at = excluded.scanned_at,
                duplicados = COALESCE(excluded.duplicados, chat_video_counts.duplicados),
                indexados = COALESCE(excluded.indexados, chat_video_counts.indexados)
            """,
            (chat_id, videos_count, scanned_at, duplicados, idx_value),
        )
        await db.commit()


async def db_get_chat_scan_meta(chat_id: int) -> dict | None:
    """Obtiene videos_count, scanned_at, duplicados e indexados del chat si existen."""
    async with get_db() as db:
        async with db.execute(
            "SELECT videos_count, scanned_at, duplicados, indexados FROM chat_video_counts WHERE chat_id = ? LIMIT 1",
            (chat_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "videos_count": row[0],
                "scanned_at": row[1],
                "duplicados": row[2],
                "indexados": row[3],
            }


async def db_get_chat_folders(chat_id: int) -> list[int]:
    """Obtiene las carpetas a las que pertenece un chat de forma ASÍNCRONA."""
    async with get_db() as db:
        async with db.execute("SELECT folder_id FROM chat_folders WHERE chat_id = ?", (chat_id,)) as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]


async def db_add_chat_folder(chat_id: int, folder_id: int) -> None:
    """Añade una relación chat-carpeta (Asíncrono)."""
    async with get_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO chat_folders (chat_id, folder_id) VALUES (?, ?)",
            (chat_id, folder_id),
        )
        await db.commit()