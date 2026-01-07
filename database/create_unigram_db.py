import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "unigram.db"


def crear_base_unigram():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        #"""    "archivo": "5807890992262553306_109.jpg",
        #"tipo": "thumbnail",
        #"fecha_escaneo": "2026-01-03 18:18:49",
        #"encontrado": true,
        #"canal_id": -5070998016,
        #"msg_id_global": 109246"""

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cacheo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                archivo TEXT NOT NULL,
                tipo TEXT,
                fecha_escaneo TEXT,
                encontrado INTEGER,
                canal_id INTEGER,
                msg_id_global INTEGER,
                tamano_bytes INTEGER,
                duracion_segundos REAL,
                en_servidor INTEGER,
                unique_id TEXT NULL,
                creado_en TEXT DEFAULT (datetime('now'))
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_cacheo_archivo
            ON cacheo(archivo)
            """
        )
        # Asegurar columnas nuevas si la tabla ya existía
        cur.execute("PRAGMA table_info(cacheo)")
        existentes = {row[1] for row in cur.fetchall()}
        if "tamano_bytes" not in existentes:
            cur.execute("ALTER TABLE cacheo ADD COLUMN tamano_bytes INTEGER")
        if "duracion_segundos" not in existentes:
            cur.execute("ALTER TABLE cacheo ADD COLUMN duracion_segundos REAL")
        if "en_servidor" not in existentes:
            cur.execute("ALTER TABLE cacheo ADD COLUMN en_servidor INTEGER")
        conn.commit()
        print(f"✅ Base de datos creada en: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    crear_base_unigram()
