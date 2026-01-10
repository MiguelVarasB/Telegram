import sqlite3
from pathlib import Path
import argparse
import json
import sys

# Asegurar import de config
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DB_PATH  # noqa: E402


def listar_mensajes(chat_id: int) -> None:
    db_path = Path(DB_PATH)
    if not db_path.exists():
        print(f"No existe la BD en {db_path}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT COUNT(*), MIN(message_id), MAX(message_id) FROM video_messages WHERE chat_id=?",
        (chat_id,),
    )
    count, min_id, max_id = cur.fetchone()
    print(f"chat_id={chat_id} | rows={count} | min_id={min_id} | max_id={max_id}")

    cur.execute(
        "SELECT id, message_id, video_id, date FROM video_messages WHERE chat_id=? ORDER BY message_id DESC",
        (chat_id,),
    )
    rows = cur.fetchall()
    for r in rows:
        print(dict(r))

    conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lista mensajes de video_messages para un chat_id")
    parser.add_argument("chat_id", type=int, help="chat_id a listar")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    listar_mensajes(args.chat_id)
