import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config import DB_PATH

def main():
    db_path = Path(DB_PATH)
    if not db_path.exists():
        print(f"No existe la BD en {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Resetear indexados a 0 antes de recalcular
        cur.execute("UPDATE chat_video_counts SET indexados = 0")
        conn.commit()

        # Asegurar columna indexados (por si la migración aún no corrió)
        cur.execute("PRAGMA table_info(chat_video_counts)")
        cols = [row[1] for row in cur.fetchall()]
        if "indexados" not in cols:
            cur.execute("ALTER TABLE chat_video_counts ADD COLUMN indexados INTEGER DEFAULT 0")
            conn.commit()

        # Construir mapa de indexados reales desde videos_telegram
        cur.execute("SELECT chat_id, COUNT(*) FROM videos_telegram GROUP BY chat_id")
        counts = dict(cur.fetchall())

        # Actualizar tabla chat_video_counts con los nuevos indexados
        updates = [(counts.get(chat_id, 0), chat_id) for chat_id in counts.keys()]
        # También cubrir chats existentes en chat_video_counts sin videos registrados
        cur.execute("SELECT chat_id FROM chat_video_counts")
        for (chat_id,) in cur.fetchall():
            if chat_id not in counts:
                updates.append((0, chat_id))

        cur.executemany("UPDATE chat_video_counts SET duplicados = 0, indexados = ? WHERE chat_id = ?", updates)
        conn.commit()
        print(f"Actualizados {len(updates)} registros en chat_video_counts.indexados")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
