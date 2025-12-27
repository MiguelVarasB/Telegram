"""
Conexión y configuración de la base de datos (Versión Asíncrona con aiosqlite).
"""
import aiosqlite
import sqlite3 # Mantenemos para compatibilidad legacy si es necesario
from config import DB_PATH

def get_connection() -> sqlite3.Connection:
    """
    DEPRECADO: Usar solo para scripts de mantenimiento o migraciones síncronas.
    Retorna una conexión síncrona a la base de datos.
    """
    return sqlite3.connect(DB_PATH)

async def init_db():
    """Crea las tablas base si no existen (Asíncrono)."""
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous = NORMAL")
        await db.execute("PRAGMA busy_timeout = 5000")
        # Tabla de chats
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                name TEXT,
                type TEXT,
                photo_id TEXT,
                username TEXT,
                raw_json TEXT,
                last_message_date TEXT,
                updated_at TEXT
            )
        """)
        
        # Tabla de conteos de videos por chat
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_video_counts (
                chat_id INTEGER PRIMARY KEY,
                videos_count INTEGER DEFAULT 0,
                scanned_at TEXT
            )
        """)
        
        # Tabla de relación chat-carpeta
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_folders (
                chat_id INTEGER,
                folder_id INTEGER,
                PRIMARY KEY (chat_id, folder_id)
            )
        """)
        
        # Migración al vuelo: Asegurar columna last_message_date
        # aiosqlite no tiene PRAGMA table_info fácil como cursor.fetchall directo a veces
        # pero podemos hacerlo así:
        async with db.execute("PRAGMA table_info(chats)") as cursor:
            columns = [row[1] async for row in cursor]
        
        if "last_message_date" not in columns:
            print(" Migrando BD: Agregando columna last_message_date...")
            await db.execute("ALTER TABLE chats ADD COLUMN last_message_date TEXT")

        # Asegurar columna watch_later en videos_telegram si existe la tabla
        try:
            async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
                video_cols = [row[1] async for row in cursor]
            if "watch_later" not in video_cols:
                print(" Migrando BD: Agregando columna watch_later a videos_telegram...")
                await db.execute("ALTER TABLE videos_telegram ADD COLUMN watch_later INTEGER DEFAULT 0")
        except Exception:
            # Si la tabla aún no existe, ignoramos silenciosamente
            pass

        await db.commit()
        print(" Base de datos inicializada (Async)")