"""
Operaciones de base de datos relacionadas con videos.
Versión Asíncrona TOTAL (Non-blocking).
"""
import json
import aiosqlite
from config import DB_PATH

# Nota: Ya no importamos get_connection síncrono para estas funciones

_VIDEO_MESSAGES_RAW_JSON_DROPPED = False
_VIDEO_MESSAGES_CAPTION_ENTITIES_DROPPED = False


async def _ensure_video_messages_table(db: aiosqlite.Connection) -> None:
    global _VIDEO_MESSAGES_RAW_JSON_DROPPED, _VIDEO_MESSAGES_CAPTION_ENTITIES_DROPPED

    await db.execute("""
        CREATE TABLE IF NOT EXISTS video_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            date TEXT,
            from_user_id INTEGER,
            from_username TEXT,
            from_is_bot INTEGER,
            media_type TEXT,
            views INTEGER,
            forwards INTEGER,
            outgoing INTEGER,
            reply_to_message_id INTEGER,
            forward_from_chat_id INTEGER,
            forward_from_chat_title TEXT,
            forward_from_message_id INTEGER,
            forward_date TEXT,
            caption TEXT,
            UNIQUE (chat_id, message_id)
        )
    """)

    if _VIDEO_MESSAGES_RAW_JSON_DROPPED and _VIDEO_MESSAGES_CAPTION_ENTITIES_DROPPED:
        return

    async with db.execute("PRAGMA table_info(video_messages)") as cursor:
        cols = [row[1] async for row in cursor]

    needs_migration = False
    if "raw_json" in cols:
        needs_migration = True
    if "caption_entities" in cols:
        needs_migration = True

    if not needs_migration:
        _VIDEO_MESSAGES_RAW_JSON_DROPPED = True
        _VIDEO_MESSAGES_CAPTION_ENTITIES_DROPPED = True
        return

    migrate_sql = """
    BEGIN IMMEDIATE;
    CREATE TABLE video_messages_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id TEXT NOT NULL,
        chat_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL,
        date TEXT,
        from_user_id INTEGER,
        from_username TEXT,
        from_is_bot INTEGER,
        media_type TEXT,
        views INTEGER,
        forwards INTEGER,
        outgoing INTEGER,
        reply_to_message_id INTEGER,
        forward_from_chat_id INTEGER,
        forward_from_chat_title TEXT,
        forward_from_message_id INTEGER,
        forward_date TEXT,
        caption TEXT,
        UNIQUE (chat_id, message_id)
    );
    INSERT INTO video_messages_new (
        id, video_id, chat_id, message_id, date,
        from_user_id, from_username, from_is_bot,
        media_type, views, forwards, outgoing, reply_to_message_id,
        forward_from_chat_id, forward_from_chat_title, forward_from_message_id, forward_date,
        caption
    )
    SELECT
        id, video_id, chat_id, message_id, date,
        from_user_id, from_username, from_is_bot,
        media_type, views, forwards, outgoing, reply_to_message_id,
        forward_from_chat_id, forward_from_chat_title, forward_from_message_id, forward_date,
        caption
    FROM video_messages;
    DROP TABLE video_messages;
    ALTER TABLE video_messages_new RENAME TO video_messages;
    COMMIT;
    """

    await db.executescript(migrate_sql)
    _VIDEO_MESSAGES_RAW_JSON_DROPPED = True
    _VIDEO_MESSAGES_CAPTION_ENTITIES_DROPPED = True


async def db_upsert_video(video_data: dict) -> None:
    """Inserta o actualiza un video en la tabla videos_telegram (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO videos_telegram (
                id, chat_id, message_id, file_id, file_unique_id, nombre, caption,
                tamano_bytes, fecha_mensaje, duracion, ancho, alto, es_vertical,
                mime_type, views, outgoing
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                file_id = excluded.file_id,
                nombre = excluded.nombre,
                caption = excluded.caption,
                tamano_bytes = excluded.tamano_bytes,
                fecha_mensaje = excluded.fecha_mensaje,
                duracion = excluded.duracion,
                ancho = excluded.ancho,
                alto = excluded.alto,
                es_vertical = excluded.es_vertical,
                mime_type = excluded.mime_type,
                views = excluded.views,
                outgoing = excluded.outgoing
        """, (
            video_data.get("file_unique_id"),
            video_data.get("chat_id"),
            video_data.get("message_id"),
            video_data.get("file_id"),
            video_data.get("file_unique_id"),
            video_data.get("nombre"),
            video_data.get("caption"),
            video_data.get("tamano_bytes"),
            video_data.get("fecha_mensaje"),
            video_data.get("duracion", 0),
            video_data.get("ancho", 0),
            video_data.get("alto", 0),
            1 if video_data.get("alto", 0) > video_data.get("ancho", 0) else 0,
            video_data.get("mime_type"),
            video_data.get("views", 0),
            1 if video_data.get("outgoing") else 0,
        ))
        await db.commit()


async def db_upsert_video_message(message_data: dict) -> None:
    """Inserta o actualiza metadatos extendidos del mensaje (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Aseguramos la tabla aquí también por si acaso (o mover a init_db)
        await _ensure_video_messages_table(db)

        from_user = message_data.get("from_user") or {}
        forward_from_chat = message_data.get("forward_from_chat") or {}

        await db.execute("""
            INSERT INTO video_messages (
                video_id, chat_id, message_id, date, from_user_id, from_username, from_is_bot,
                media_type, views, forwards, outgoing, reply_to_message_id,
                forward_from_chat_id, forward_from_chat_title, forward_from_message_id,
                forward_date, caption
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                video_id = excluded.video_id,
                date = excluded.date,
                from_user_id = excluded.from_user_id,
                from_username = excluded.from_username,
                from_is_bot = excluded.from_is_bot,
                media_type = excluded.media_type,
                views = excluded.views,
                forwards = excluded.forwards,
                outgoing = excluded.outgoing,
                reply_to_message_id = excluded.reply_to_message_id,
                forward_from_chat_id = excluded.forward_from_chat_id,
                forward_from_chat_title = excluded.forward_from_chat_title,
                forward_from_message_id = excluded.forward_from_message_id,
                forward_date = excluded.forward_date,
                caption = excluded.caption
        """, (
            message_data.get("video_id"),
            message_data.get("chat_id"),
            message_data.get("message_id"),
            message_data.get("date"),
            from_user.get("id"),
            from_user.get("username"),
            1 if from_user.get("is_bot") else 0 if from_user else None,
            message_data.get("media"),
            message_data.get("views"),
            message_data.get("forwards"),
            1 if message_data.get("outgoing") else 0,
            message_data.get("reply_to_message_id"),
            forward_from_chat.get("id"),
            forward_from_chat.get("title"),
            message_data.get("forward_from_message_id"),
            message_data.get("forward_date"),
            message_data.get("caption"),
        ))
        await db.commit()


async def db_get_video_messages(video_id: str) -> list[dict]:
    """Obtiene mensajes asociados a un video de forma ASÍNCRONA (Sin cambios mayores)."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Nota: La creación de tabla aquí es redundante si ya se hace arriba o en init_db,
        # pero no hace daño dejarla por seguridad.
        await _ensure_video_messages_table(db)
        async with db.execute("""
            SELECT
                chat_id, message_id, date, from_user_id, from_username, from_is_bot,
                media_type, views, forwards, outgoing, reply_to_message_id,
                forward_from_chat_id, forward_from_chat_title, forward_from_message_id,
                forward_date, caption
            FROM video_messages
            WHERE video_id = ?
            ORDER BY date
        """, (video_id,)) as cursor:
            rows = await cursor.fetchall()

    results: list[dict] = []
    for r in rows:
        results.append({
            "chat_id": r[0],
            "message_id": r[1],
            "date": r[2],
            "from_user_id": r[3],
            "from_username": r[4],
            "from_is_bot": r[5],
            "media_type": r[6],
            "views": r[7],
            "forwards": r[8],
            "outgoing": r[9],
            "reply_to_message_id": r[10],
            "forward_from_chat_id": r[11],
            "forward_from_chat_title": r[12],
            "forward_from_message_id": r[13],
            "forward_date": r[14],
            "caption": r[15],
        })
    return results


async def db_add_video_file_id(video_id: str, file_id: str, file_unique_id: str, origen: str = "scan") -> None:
    """Registra un file_id detectado para un video (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO video_file_ids (video_id, file_id, file_unique_id, origen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(video_id, file_id) DO UPDATE SET
                fecha_detectado = CURRENT_TIMESTAMP
        """, (video_id, file_id, file_unique_id, origen))
        await db.commit()

async def db_count_videos_by_chat(chat_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM videos_telegram WHERE chat_id = ?",
            (chat_id,),
        ) as cursor:
            row = await cursor.fetchone()

    if not row:
        return 0
    return int(row[0] or 0)