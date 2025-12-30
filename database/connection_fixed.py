"""
Conexión y configuración de la base de datos (Versión Asíncrona con aiosqlite).
Incluye manejo de reconexión y bloqueos.
"""
import aiosqlite
import sqlite3
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
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
            await db.isolation_level = "IMMEDIATE"
            
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
