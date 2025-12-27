import sqlite3
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "dumps" / "chats.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS videos_telegram (
    id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL,
    nombre TEXT,
    caption TEXT,
    tags_ia TEXT DEFAULT '[]',
    meta_extra TEXT,
    ruta_local TEXT,
    ruta_mega TEXT,
    tamano_bytes INTEGER,
    fecha_mod TEXT,
    fecha_mensaje TEXT,
    fecha_descarga TEXT,
    fecha_procesado TEXT DEFAULT '',
    duracion REAL DEFAULT 0,
    ancho INTEGER DEFAULT 0,
    alto INTEGER DEFAULT 0,
    es_vertical INTEGER DEFAULT 0,
    codec_video TEXT DEFAULT '',
    codec_audio TEXT DEFAULT '',
    bitrate INTEGER DEFAULT 0,
    fps REAL DEFAULT 0,
    tiene_audio INTEGER DEFAULT 1,
    version_sprite TEXT DEFAULT '',
    es_video INTEGER DEFAULT 0,
    has_sprite INTEGER DEFAULT 0,
    has_thumb INTEGER DEFAULT 0,
    en_mega INTEGER DEFAULT 1,
    oculto INTEGER DEFAULT 0,
    watch_later INTEGER DEFAULT 0,
    ffmpeg_error INTEGER DEFAULT 0,
    mime_type TEXT,
    views INTEGER DEFAULT 0,
    outgoing INTEGER DEFAULT 0,
    reply_to_message_id INTEGER,
    forwarded_from TEXT,
    sprite_path TEXT,
    thumb_path TEXT,
    download_url TEXT,
    UNIQUE (chat_id, message_id),
    UNIQUE (file_unique_id)
);

CREATE TABLE IF NOT EXISTS video_file_ids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT,
    fecha_detectado TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    origen TEXT DEFAULT 'scan',
    notas TEXT,
    FOREIGN KEY (video_id) REFERENCES videos_telegram(id) ON DELETE CASCADE,
    UNIQUE (video_id, file_id)
);
"""


def ensure_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()
    print(f"✅ Tablas videos_telegram y video_file_ids listas en {DB_PATH}")


if __name__ == "__main__":
    ensure_database()
