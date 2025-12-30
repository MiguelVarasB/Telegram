"""
Conexión y configuración de la base de datos (Versión Asíncrona con aiosqlite).
Incluye manejo de reconexión y bloqueos.
"""
import aiosqlite
import sqlite3
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, List, Tuple, Any, Dict
from config import DB_PATH

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de la base de datos
DB_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_DELAY = 0.5

# Configuración de SQLite para mejor concurrencia
SQLITE_PRAGMAS = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "busy_timeout": 5000,  # 5 segundos
    "foreign_keys": "ON",
    "temp_store": "MEMORY",
    "mmap_size": 300000000,  # 300MB
    "cache_size": -2000,  # 2000 páginas de 1KB = 2MB
}

class DatabaseConnectionError(Exception):
    """Excepción personalizada para errores de conexión a la base de datos."""
    pass

async def get_db_connection() -> aiosqlite.Connection:
    """
    Obtiene una conexión a la base de datos con configuración optimizada.
    Incluye manejo de reconexión automática.
    """
    retries = 0
    last_error = None
    
    while retries < MAX_RETRIES:
        try:
            db = await aiosqlite.connect(DB_PATH, timeout=DB_TIMEOUT)
            
            # Aplicar configuraciones PRAGMA
            for pragma, value in SQLITE_PRAGMAS.items():
                await db.execute(f"PRAGMA {pragma} = {value}")
                
            # Habilitar soporte para claves foráneas
            await db.execute("PRAGMA foreign_keys = ON")
            
            # Configurar el modo de aislamiento
            db.isolation_level = "IMMEDIATE"
            
            return db
            
        except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
            last_error = e
            retries += 1
            if retries < MAX_RETRIES:
                logger.warning(
                    f"Error de conexión a la base de datos (intento {retries}/{MAX_RETRIES}): {e}"
                )
                await asyncio.sleep(RETRY_DELAY * (2 ** (retries - 1)))  # Backoff exponencial
            else:
                logger.error("Número máximo de reintentos alcanzado")
                raise DatabaseConnectionError(
                    f"No se pudo conectar a la base de datos después de {MAX_RETRIES} intentos"
                ) from e

@asynccontextmanager
async def get_db() -> AsyncIterator[aiosqlite.Connection]:
    """
    Context manager para manejar conexiones a la base de datos.
    Se encarga de abrir y cerrar la conexión automáticamente.
    """
    db = await get_db_connection()
    try:
        yield db
    finally:
        await db.close()

@asynccontextmanager
async def transaction(db: aiosqlite.Connection) -> AsyncIterator[aiosqlite.Cursor]:
    """
    Context manager para manejar transacciones de forma segura.
    """
    cursor = await db.cursor()
    try:
        await cursor.execute("BEGIN IMMEDIATE")
        yield cursor
        await db.commit()
    except Exception as e:
        await db.rollback()
        logger.error(f"Error en la transacción: {e}")
        raise
    finally:
        await cursor.close()

def get_sync_connection() -> sqlite3.Connection:
    """
    Obtiene una conexión síncrona a la base de datos.
    SOLO para operaciones de mantenimiento o migraciones.
    """
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)
    for pragma, value in SQLITE_PRAGMAS.items():
        conn.execute(f"PRAGMA {pragma} = {value}")
    return conn

async def init_db():
    """
    Inicializa la base de datos y crea las tablas necesarias si no existen.
    """
    async with get_db() as db:
        # Crear tabla de chats
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
                scanned_at TEXT,
                duplicados INTEGER DEFAULT 0,
                indexados INTEGER DEFAULT 0
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
        
        # Tabla de videos
        await db.execute("""
            CREATE TABLE IF NOT EXISTS videos_telegram (
                chat_id INTEGER,
                message_id INTEGER,
                file_id TEXT,
                file_unique_id TEXT,
                file_name TEXT,
                file_size INTEGER,
                mime_type TEXT,
                duration INTEGER,
                width INTEGER,
                height INTEGER,
                thumb_file_id TEXT,
                thumb_width INTEGER,
                thumb_height INTEGER,
                message_date TEXT,
                oculto INTEGER DEFAULT 0,
                watch_later INTEGER DEFAULT 0,
                thumb_bytes BLOB,
                has_thumb INTEGER DEFAULT 0,
                tamano_bytes INTEGER,
                duracion INTEGER,
                nombre TEXT,
                fecha_mensaje TEXT,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        
        # Índices para mejorar el rendimiento de búsqueda
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_videos_oculto 
            ON videos_telegram(oculto)
        """)
        
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_videos_watch_later 
            ON videos_telegram(watch_later)
        """)
        
        # Aplicar migraciones
        await _run_migrations(db)
        
        await db.commit()
        logger.info("Base de datos inicializada correctamente")

async def _run_migrations(db: aiosqlite.Connection):
    """Ejecuta migraciones necesarias en la base de datos."""
    # Verificar y agregar columnas faltantes
    tables = {
        "chats": ["last_message_date"],
        "chat_video_counts": ["duplicados", "indexados"],
        "videos_telegram": ["watch_later", "thumb_bytes", "has_thumb", 
                           "tamano_bytes", "duracion", "nombre", "fecha_mensaje"]
    }
    
    for table, columns in tables.items():
        try:
            async with db.execute(f"PRAGMA table_info({table})") as cursor:
                existing_columns = [row[1] for row in await cursor.fetchall()]
                
            for column in columns:
                if column not in existing_columns:
                    logger.info(f"Añadiendo columna {column} a la tabla {table}")
                    await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} " + {
                        "last_message_date": "TEXT",
                        "duplicados": "INTEGER DEFAULT 0",
                        "indexados": "INTEGER DEFAULT 0",
                        "watch_later": "INTEGER DEFAULT 0",
                        "thumb_bytes": "BLOB",
                        "has_thumb": "INTEGER DEFAULT 0",
                        "tamano_bytes": "INTEGER",
                        "duracion": "INTEGER",
                        "nombre": "TEXT",
                        "fecha_mensaje": "TEXT"
                    }[column])
        except aiosqlite.OperationalError as e:
            logger.warning(f"No se pudo verificar la tabla {table}: {e}")
            continue