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
        # Resetear indexados a 0 antes de recalcular
        cur.execute("UPDATE chat_video_counts SET indexados = 0")
        conn.commit()

        # Asegurar columna indexados (por si la migración aún no corrió)
        cur.execute("PRAGMA table_info(chat_video_counts)")
        cols = [row[1] for row in cur.fetchall()]
        if "indexados" not in cols:
            cur.execute("ALTER TABLE chat_video_counts ADD COLUMN indexados INTEGER DEFAULT 0")
            conn.commit()

        # Construir mapa de indexados reales desde video_messages (mensajes con video por chat)
        # TODOD LOS CANALES SIN IMPORTAR SI ES MIO O ESTA INACTIVO, YA QUE ES UN RESUMEN DE LA BASE DE DATOS
        cur.execute(
            """
            SELECT vm.chat_id, COUNT(*)
            FROM video_messages vm
            JOIN chats c ON vm.chat_id = c.chat_id
            WHERE COALESCE(c.is_owner, 0) != 10
            GROUP BY vm.chat_id
            """
        )
        counts = dict(cur.fetchall())

        # Actualizar tabla chat_video_counts con los nuevos indexados
        # Primero obtener todos los chats existentes
        cur.execute("SELECT chat_id FROM chat_video_counts")
        all_chats = {row[0] for row in cur.fetchall()}
        
        # Construir updates para todos los chats
        updates = []
        for chat_id in all_chats:
            indexados_count = counts.get(chat_id, 0)
            updates.append((indexados_count, chat_id))

        # Solo actualizar indexados, NO tocar duplicados
        cur.executemany("UPDATE chat_video_counts SET indexados = ? WHERE chat_id = ?", updates)
        conn.commit()
        print(f"Actualizados {len(updates)} registros en chat_video_counts.indexados")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
