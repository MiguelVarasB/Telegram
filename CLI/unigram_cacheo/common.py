import os
import sqlite3
import subprocess
import json
from pathlib import Path
from typing import Tuple

from sqlcipher3 import dbapi2 as sqlcipher

# --- CONFIGURACIÓN COMPARTIDA ---
MASTER_KEY = "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63"
Path_UNIGRAM = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0"
DB_UNIGRAM = os.path.join(Path_UNIGRAM, "db.sqlite")  # base de datos de unigram
BASE_DIR = Path(__file__).resolve().parents[2]
DIR_DB = BASE_DIR / "database"
# Base de datos local (unigram.db) siempre dentro de /database
DB_LOCAL = DIR_DB / "unigram.db"

# carpetas cacheadas de unigram
CARPETAS = {
    "thumbnail": os.path.join(Path_UNIGRAM, "thumbnails"),
    "video": os.path.join(Path_UNIGRAM, "videos"),
}


def preparar_base_local() -> Tuple[sqlite3.Connection, set]:
    """Crea unigram.db (si falta) y trae archivos ya indexados."""
    from database.create_unigram_db import crear_base_unigram

    crear_base_unigram()
    conn = sqlite3.connect(DB_LOCAL)
    cur = conn.cursor()
    cur.execute("SELECT archivo FROM cacheo")
    existentes = {row[0] for row in cur.fetchall()}
    return conn, existentes


def obtener_duracion_video(ruta: str) -> float | None:
    """Devuelve duración en segundos usando ffprobe si está disponible."""
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            ruta,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if res.returncode != 0:
            return None
        data = json.loads(res.stdout)
        dur = data.get("format", {}).get("duration")
        return float(dur) if dur is not None else None
    except subprocess.TimeoutExpired:
        return None
    except Exception:
        return None


def cargar_mensajes_unigram():
    conn = sqlcipher.connect(DB_UNIGRAM)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
    cursor.execute("PRAGMA cipher_compatibility = 4;")
    cursor.execute("SELECT message_id, dialog_id, data FROM messages")
    todos = cursor.fetchall()
    return conn, todos
