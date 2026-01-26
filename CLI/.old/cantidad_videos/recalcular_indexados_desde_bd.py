import sqlite3
import sys
from pathlib import Path

"""Resumen: Recalcula chat_video_counts.indexados desde videos_telegram y reinicia duplicados a 0, garantizando la columna indexados si falta."""
"""Objetivo: Calcular la cantidad de videos indexados por chat  usando para ello la tabla video_messages (la tabla videos_telegram no se usa ya que ahi se guardan solo videos unicos)"""
ROOT_DIR = Path(__file__).resolve().parents[2]  # raíz del proyecto
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
        
        # Asegurar columna indexados (por si la migración aún no corrió)
        cur.execute("PRAGMA table_info(chat_video_counts)")
        cols = [row[1] for row in cur.fetchall()]
        if "indexados" not in cols:
            cur.execute("ALTER TABLE chat_video_counts ADD COLUMN indexados INTEGER DEFAULT 0")
            conn.commit()

        # Actualizar indexados con una sola query optimizada
        # Usa subquery correlacionada para calcular y actualizar en un solo paso
        cur.execute(
            """
            UPDATE chat_video_counts
            SET indexados = (
                SELECT COUNT(*)
                FROM video_messages vm
                JOIN chats c ON vm.chat_id = c.chat_id
                WHERE vm.chat_id = chat_video_counts.chat_id
                  AND COALESCE(c.is_owner, 0) != 10
            )
            """
        )
        affected = cur.rowcount
        conn.commit()
        print(f"Actualizados {affected} registros en chat_video_counts.indexados")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
