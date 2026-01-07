import argparse
import os
import sqlite3
import time
from pathlib import Path
from typing import Iterable

import imagehash
from PIL import Image

from config import DB_PATH, THUMB_FOLDER


THUMB_EXT = ".webp"
LOCK_RETRIES = 5
LOCK_SLEEP_SECONDS = 0.8


def configure_connection(conn: sqlite3.Connection) -> None:
    """Ajustes para reducir bloqueos."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 30000")  # 30s de espera por lock


def ensure_thumb_phash_column(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(videos_telegram)")
    cols = [row[1] for row in cur.fetchall()]
    if "thumb_phash" not in cols:
        conn.execute("ALTER TABLE videos_telegram ADD COLUMN thumb_phash TEXT;")
        conn.commit()


def iter_pending_rows(conn: sqlite3.Connection, limit: int | None, offset: int) -> Iterable[tuple[int, str]]:
    sql = [
        "SELECT chat_id, file_unique_id",
        "FROM videos_telegram",
        "WHERE oculto = 0",
        "  AND has_thumb > 0",
        "  AND (thumb_phash IS NULL OR thumb_phash = '')",
        "ORDER BY fecha_mensaje DESC, message_id DESC",
    ]
    params: list[int] = []
    if limit is not None:
        sql.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
    elif offset:
        # SQLite requiere LIMIT si hay OFFSET; usamos un LIMIT grande
        sql.append("LIMIT -1 OFFSET ?")
        params.append(offset)
    query = "\n".join(sql)
    for chat_id, file_unique_id in conn.execute(query, params):
        yield int(chat_id), (file_unique_id or "").strip()


def compute_phash(path: Path) -> str | None:
    try:
        with Image.open(path) as img:
            return str(imagehash.phash(img))
    except Exception:
        return None


def process_batch(conn: sqlite3.Connection, rows: list[tuple[int, str]], thumb_root: Path) -> tuple[int, int]:
    ok = fail = 0
    for chat_id, file_unique_id in rows:
        if not file_unique_id:
            fail += 1
            continue
        thumb_path = thumb_root / str(chat_id) / f"{file_unique_id}{THUMB_EXT}"
        if not thumb_path.exists():
            fail += 1
            continue
        ph = compute_phash(thumb_path)
        if ph is None:
            fail += 1
            continue
        for attempt in range(LOCK_RETRIES):
            try:
                conn.execute(
                    "UPDATE videos_telegram SET thumb_phash = ? WHERE chat_id = ? AND file_unique_id = ?",
                    (ph, chat_id, file_unique_id),
                )
                break
            except sqlite3.OperationalError as e:
                # Reintento si está locked
                if "locked" in str(e).lower() and attempt + 1 < LOCK_RETRIES:
                    time.sleep(LOCK_SLEEP_SECONDS * (attempt + 1))
                    continue
                raise
        ok += 1
    return ok, fail


def run(limit: int | None, offset: int, batch_size: int) -> None:
    thumb_root = Path(THUMB_FOLDER)
    if not thumb_root.exists():
        raise SystemExit(f"Carpeta de thumbs no existe: {thumb_root}")

    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        configure_connection(conn)
        ensure_thumb_phash_column(conn)

        pending_iter = iter_pending_rows(conn, limit=limit, offset=offset)
        total_ok = total_fail = total_seen = 0
        batch: list[tuple[int, str]] = []

        for row in pending_iter:
            batch.append(row)
            total_seen += 1
            if len(batch) >= batch_size:
                ok, fail = process_batch(conn, batch, thumb_root)
                total_ok += ok
                total_fail += fail
                conn.commit()
                batch.clear()
                print(f"Procesados {total_seen} (ok={total_ok}, fail={total_fail})")

        if batch:
            ok, fail = process_batch(conn, batch, thumb_root)
            total_ok += ok
            total_fail += fail
            conn.commit()

    print("\nResumen:")
    print(f"  Total registros leídos: {total_seen}")
    print(f"  Hashes guardados:      {total_ok}")
    print(f"  Fallos/sin thumb:      {total_fail}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precálculo de pHash para thumbs visibles (oculto=0)")
    parser.add_argument("--limit", type=int, default=None, help="Límite de filas a procesar")
    parser.add_argument("--offset", type=int, default=0, help="Offset inicial")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamaño de batch para commits")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(limit=args.limit, offset=args.offset, batch_size=args.batch_size)
