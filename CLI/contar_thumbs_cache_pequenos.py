import os
import sqlite3
from pathlib import Path

from config import DB_PATH, THUMB_FOLDER

# Umbral de tamaño: 3 KB
THRESHOLD_BYTES = 3 * 1024
HAS_CACHE_VAL = 5


def run():
    thumb_root = Path(THUMB_FOLDER)
    if not thumb_root.exists():
        print(f"❌ Carpeta de thumbs no existe: {thumb_root}")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT chat_id, file_unique_id
            FROM videos_telegram
            WHERE has_cache = ? AND file_unique_id IS NOT NULL
            """,
            (HAS_CACHE_VAL,),
        )
        rows = cur.fetchall()
        total = len(rows)
        pequenos = 0
        ejemplos = []

        print(f"🔎 Revisando {total} videos con has_cache={HAS_CACHE_VAL}...")

        for idx, (chat_id, fuid) in enumerate(rows, start=1):
            if idx % 5000 == 0 or idx == total:
                print(f"  Procesados {idx}/{total}")

            thumb_path = thumb_root / str(chat_id) / f"{fuid}.webp"
            try:
                if thumb_path.exists():
                    size = thumb_path.stat().st_size
                    if size < THRESHOLD_BYTES:
                        pequenos += 1
                        if len(ejemplos) < 20:
                            ejemplos.append((chat_id, fuid, size))
            except Exception:
                # Ignorar errores por archivos corruptos/permisos
                continue

        print("\nResumen de thumbs < 3KB:")
        print(f"  Total revisados: {total}")
        print(f"  Thumbs <3KB:    {pequenos}")
        if ejemplos:
            print("  Ejemplos (chat_id, fuid, bytes):")
            for chat_id, fuid, size in ejemplos:
                print(f"   - {chat_id}/{fuid}.webp -> {size} bytes")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
