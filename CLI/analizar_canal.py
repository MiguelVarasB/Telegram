import argparse
import asyncio
import sqlite3
import sys
from pathlib import Path

from pyrogram import enums

# Asegurar import desde la raíz del proyecto
# Desde CLI/analizar_canal.py, la raíz del repo es parents[1]
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import DB_PATH  # noqa: E402
from services.telegram_client import get_client  # noqa: E402


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_chat_stats(conn: sqlite3.Connection, chat_id: int):
    cur = conn.execute(
        """
        SELECT
            chat_id,
            COALESCE(indexados, 0) AS indexados,
            COALESCE(videos_count, 0) AS videos_count,
            COALESCE(duplicados, 0) AS duplicados,
            COALESCE(videos_count, 0) - COALESCE(duplicados, 0) AS total_unicos,
            (COALESCE(videos_count, 0) - COALESCE(duplicados, 0) - COALESCE(indexados, 0)) AS faltantes
        FROM chat_video_counts
        WHERE chat_id = ?
        """,
        (chat_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_video_messages_stats(conn: sqlite3.Connection, chat_id: int):
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            MIN(message_id) AS min_id,
            MAX(message_id) AS max_id
        FROM video_messages
        WHERE chat_id = ?
        """,
        (chat_id,),
    )
    row = cur.fetchone()
    total = row["total"] or 0
    min_id = row["min_id"]
    max_id = row["max_id"]
    return total, min_id, max_id


def fetch_sample_messages(conn: sqlite3.Connection, chat_id: int, limit: int = 15):
    cur = conn.execute(
        """
        SELECT id, message_id, video_id, date
        FROM video_messages
        WHERE chat_id = ?
        ORDER BY message_id DESC
        LIMIT ?
        """,
        (chat_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_videos_stats(conn: sqlite3.Connection, chat_id: int):
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT file_unique_id) AS total_unique,
            COUNT(DISTINCT message_id) AS total_message_ids
        FROM videos_telegram
        WHERE chat_id = ?
        """,
        (chat_id,),
    )
    row = cur.fetchone()
    return {
        "videos_rows": row["total_rows"] or 0,
        "videos_unique": row["total_unique"] or 0,
        "videos_msg_ids": row["total_message_ids"] or 0,
    }


async def fetch_telegram_live(chat_id: int):
    client = get_client(clone_for_cli=True)
    await client.start()
    try:
        chat = await client.get_chat(chat_id)
        try:
            history_count = await client.get_chat_history_count(chat_id)
            history_count_err = None
        except Exception as e:
            history_count = None
            history_count_err = str(e)

        videos_count = None
        videos_count_err = None
        try:
            videos_count = await client.search_messages_count(chat_id, filter=enums.MessagesFilter.VIDEO)
        except Exception as e1:
            videos_count_err = f"search_messages_count: {e1}"
            # Fallback: contar iterando hasta 10k
            try:
                vc = 0
                async for _ in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=10000):
                    vc += 1
                videos_count = vc
                videos_count_err = f"{videos_count_err} | fallback_ok_hasta_10k"
            except Exception as e2:
                videos_count_err = f"{videos_count_err} | fallback_error: {e2}"

        sample_live = []
        try:
            async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=50):
                sample_live.append(
                    {
                        "message_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "file_name": getattr(m.video, "file_name", None) if m.video else None,
                    }
                )
        except Exception as e:
            sample_live = [f"error: {e}"]

        sample_history = []
        try:
            async for m in client.get_chat_history(chat_id, limit=100):
                if not m.video:
                    continue
                sample_history.append(
                    {
                        "message_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "file_name": getattr(m.video, "file_name", None),
                    }
                )
        except Exception as e:
            sample_history = [f"error: {e}"]

        return {
            "id": chat.id,
            "title": chat.title,
            "username": chat.username,
            "type": str(chat.type),
            "members_count": getattr(chat, "members_count", None),
            "description": chat.description,
            "dc_id": getattr(chat, "dc_id", None),
            "is_verified": getattr(chat, "is_verified", None),
            "is_restricted": getattr(chat, "is_restricted", None),
            "is_scam": getattr(chat, "is_scam", None),
            "is_fake": getattr(chat, "is_fake", None),
            "is_support": getattr(chat, "is_support", None),
            "is_forum": getattr(chat, "is_forum", None),
            "has_protected_content": getattr(chat, "has_protected_content", None),
            "history_count": history_count,
            "history_count_error": history_count_err,
            "videos_count_api": videos_count,
            "videos_count_api_error": videos_count_err,
            "sample_live_videos": sample_live,
            "sample_history_videos": sample_history,
        }
    finally:
        try:
            await client.stop()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Audita un canal en DB para comparar conteos.")
    parser.add_argument("chat_id", type=int, help="ID numérico del canal/grupo (p.ej. -100...)")
    parser.add_argument("--limit", type=int, default=15, help="Cantidad de mensajes de muestra a listar (default 15)")
    parser.add_argument("--sin-telegram", action="store_true", help="No consultar a Telegram (solo DB).")
    args = parser.parse_args()

    conn = get_conn()

    stats = fetch_chat_stats(conn, args.chat_id)
    total_msgs, min_id, max_id = fetch_video_messages_stats(conn, args.chat_id)
    videos_stats = fetch_videos_stats(conn, args.chat_id)
    sample = fetch_sample_messages(conn, args.chat_id, args.limit)

    print("\n== chat_video_counts ==")
    if stats:
        print(stats)
    else:
        print("(sin registro en chat_video_counts)")

    print("\n== video_messages ==")
    print({
        "total": total_msgs,
        "min_id": min_id,
        "max_id": max_id,
    })

    print("\n== videos_telegram ==")
    print(videos_stats)

    print(f"\n== Muestra últimas {len(sample)} filas de video_messages ==")
    for row in sample:
        print(row)

    if not args.sin_telegram:
        print("\n== Telegram (live) ==")
        tg_info = asyncio.run(fetch_telegram_live(args.chat_id))
        print(tg_info)

    conn.close()


if __name__ == "__main__":
    main()
