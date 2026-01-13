"""
Precalcula y guarda el tamaño en bytes de los thumbs (videos_telegram).

- Crea la columna thumb_bytes si no existe.
- Procesa thumbs con has_thumb > 0, ordenados como en duplicados
  (fecha_mensaje DESC, message_id DESC), con límite configurable (default 1000).
- Guarda el tamaño en bytes (entero) para filtrado rápido en SQL.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiosqlite

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH, THUMB_FOLDER
from utils.database_helpers import ensure_column


async def fetch_rows(db: aiosqlite.Connection, limit: int):
    sql = """
    SELECT chat_id, message_id, file_unique_id
    FROM videos_telegram
    WHERE has_thumb > 0 AND oculto = 0 AND (thumb_bytes IS NULL OR thumb_bytes = 0)
    ORDER BY fecha_mensaje DESC, message_id DESC
    LIMIT ?
    """
    async with db.execute(sql, (limit,)) as cursor:
        return await cursor.fetchall()


async def update_sizes(db: aiosqlite.Connection, rows) -> int:
    updates = []
    for r in rows:
        chat_id = int(r[0])
        message_id = int(r[1])
        video_id = (r[2] or "").strip()
        if not video_id:
            continue
        path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
        try:
            size_bytes = int(os.path.getsize(path))
        except Exception:
            size_bytes = 0
        updates.append((size_bytes, chat_id, message_id))

    if not updates:
        return 0

    await db.execute("BEGIN IMMEDIATE;")
    await db.executemany(
        """
        UPDATE videos_telegram
        SET thumb_bytes = ?
        WHERE chat_id = ? AND message_id = ?
        """,
        updates,
    )
    await db.commit()
    return len(updates)


async def main():
    parser = argparse.ArgumentParser(description="Precalcular tamaño de thumbs en videos_telegram.")
    parser.add_argument("--limit", type=int, default=100000, help="Máximo de filas a procesar (default 10000).")
    args = parser.parse_args()

    limit = max(1, args.limit)

    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_column(db, "videos_telegram", "thumb_bytes", "INTEGER")
        rows = await fetch_rows(db, limit)
        print(f"Filas a procesar: {len(rows)}")
        updated = await update_sizes(db, rows)
        print(f"Tamaños guardados: {updated}")


if __name__ == "__main__":
    asyncio.run(main())
