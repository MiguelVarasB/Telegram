import sqlite3
import sys
from pathlib import Path

"""Resumen: Recalcula chat_video_counts.indexados y chat_video_counts.duplicados desde video_messages."""
"""Objetivo: Contar videos indexados (incluyendo repetidos) y duplicados por chat usando video_messages (no videos_telegram)."""
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
        # Asegurar columnas requeridas
        cur.execute("PRAGMA table_info(chat_video_counts)")
        cols = [row[1] for row in cur.fetchall()]
        if "indexados" not in cols:
            cur.execute("ALTER TABLE chat_video_counts ADD COLUMN indexados INTEGER DEFAULT 0")
            conn.commit()
        if "duplicados" not in cols:
            cur.execute("ALTER TABLE chat_video_counts ADD COLUMN duplicados INTEGER DEFAULT 0")
            conn.commit()

        # Resetear valores antes de recalcular
        cur.execute("UPDATE chat_video_counts SET indexados = 0, duplicados = 0")
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
        counts_indexados = dict(cur.fetchall())

        # Conteo de duplicados por chat (file_unique_id con más de 1 mensaje)
        cur.execute(
            """
            WITH base AS (
                SELECT vm.chat_id, vm.video_id, COUNT(*) AS n
                FROM video_messages vm
                JOIN chats c ON vm.chat_id = c.chat_id
                WHERE COALESCE(c.is_owner, 0) != 10
                GROUP BY vm.chat_id, vm.video_id
                HAVING n > 1
            )
            SELECT chat_id, COUNT(*) AS dupes
            FROM base
            GROUP BY chat_id
            """
        )
        counts_dupes = dict(cur.fetchall())

        # Actualizar tabla chat_video_counts con los nuevos indexados
        # Primero obtener todos los chats existentes
        cur.execute("SELECT chat_id FROM chat_video_counts")
        all_chats = {row[0] for row in cur.fetchall()}
        
        # Construir updates para todos los chats
        updates = []
        for chat_id in all_chats:
            indexados_count = counts_indexados.get(chat_id, 0)
            dupes_count = counts_dupes.get(chat_id, 0)
            updates.append((indexados_count, dupes_count, chat_id))

        cur.executemany(
            "UPDATE chat_video_counts SET indexados = ?, duplicados = ? WHERE chat_id = ?",
            updates,
        )
        conn.commit()
        print(f"Actualizados {len(updates)} registros en chat_video_counts (indexados y duplicados)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
