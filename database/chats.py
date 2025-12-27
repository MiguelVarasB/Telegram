"""
Operaciones de base de datos relacionadas con chats.
Versión Async TOTAL (aiosqlite).
"""
import json
import datetime
import aiosqlite
from config import DB_PATH

async def db_upsert_chat_basic(
    chat_id: int,
    name: str | None,
    chat_type: str | None,
    username: str | None,
    raw_json: str | None = None,
    last_message_date: str | None = None,
) -> None:
    """Inserta/actualiza un chat con los campos mínimos (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO chats (chat_id, name, type, photo_id, username, raw_json, last_message_date, updated_at)
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                username=excluded.username,
                raw_json=excluded.raw_json,
                last_message_date=COALESCE(excluded.last_message_date, chats.last_message_date),
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                name or "Sin Nombre",
                chat_type,
                username,
                raw_json,
                last_message_date,
                datetime.datetime.utcnow().isoformat(),
            ),
        )
        await db.commit()


async def db_upsert_chat_from_ci(ci, last_message_date: str | None = None):
    """Guarda/actualiza un chat resuelto en la base de datos (Asíncrono)."""
    chat_id = ci.id
    name = ci.title or getattr(ci, "first_name", None) or "Sin Nombre"
    chat_type = str(ci.type).replace("ChatType.", "") if getattr(ci, "type", None) else None
    photo_id = ci.photo.small_file_id if getattr(ci, "photo", None) else None
    username = getattr(ci, "username", None)
    
    try:
        raw_dict = json.loads(str(ci))
        raw_json = json.dumps(raw_dict, ensure_ascii=False)
    except Exception:
        raw_json = str(ci)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO chats (chat_id, name, type, photo_id, username, raw_json, last_message_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                photo_id=excluded.photo_id,
                username=excluded.username,
                raw_json=excluded.raw_json,
                last_message_date=COALESCE(excluded.last_message_date, chats.last_message_date),
                updated_at=excluded.updated_at
        """, (
            chat_id,
            name,
            chat_type,
            photo_id,
            username,
            raw_json,
            last_message_date,
            datetime.datetime.utcnow().isoformat(),
        ))
        await db.commit()


async def db_get_chat(chat_id: int) -> dict | None:
    """Obtiene un chat guardado por ID de forma ASÍNCRONA."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT chat_id, name, type, photo_id, username, raw_json, last_message_date FROM chats WHERE chat_id = ?",
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
            }


async def db_upsert_chat_video_count(chat_id: int, videos_count: int, scanned_at: str | None = None) -> None:
    """Inserta/actualiza el conteo de videos por chat."""
    scanned_at = scanned_at or datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO chat_video_counts (chat_id, videos_count, scanned_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                videos_count = excluded.videos_count,
                scanned_at = excluded.scanned_at
            """,
            (chat_id, videos_count, scanned_at),
        )
        await db.commit()


async def db_get_chat_folders(chat_id: int) -> list[int]:
    """Obtiene las carpetas a las que pertenece un chat de forma ASÍNCRONA."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT folder_id FROM chat_folders WHERE chat_id = ?", (chat_id,)) as cursor:
            rows = await cursor.fetchall()
            return [r[0] for r in rows]


async def db_add_chat_folder(chat_id: int, folder_id: int) -> None:
    """Añade una relación chat-carpeta (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO chat_folders (chat_id, folder_id) VALUES (?, ?)",
            (chat_id, folder_id),
        )
        await db.commit()