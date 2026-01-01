import argparse
import sqlite3
from pathlib import Path

from config import DB_PATH


def get_duplicates(limit: int | None = None):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        sql = """
            SELECT file_unique_id, COUNT(*) AS c
            FROM videos_telegram
            GROUP BY file_unique_id
            HAVING COUNT(*) > 1
            ORDER BY c DESC, file_unique_id
        """
        if limit:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def get_chats_more_than_indexed(limit: int | None = None):
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        sql = """
            SELECT
                chat_id,
                videos_count,
                COALESCE(indexados, 0) AS indexados,
                COALESCE(duplicados, 0) AS duplicados,
                (videos_count - COALESCE(indexados, 0)) AS faltantes
            FROM chat_video_counts
            WHERE videos_count > COALESCE(indexados, 0)
            ORDER BY faltantes DESC
        """
        if limit:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def get_duplicates_by_file_id(limit: int | None = None):
    """Duplicados considerando file_id (por si el UNIQUE de file_unique_id evita contar)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        sql = """
            SELECT file_id, COUNT(*) AS c
            FROM videos_telegram
            GROUP BY file_id
            HAVING COUNT(*) > 1
            ORDER BY c DESC, file_id
        """
        if limit:
            sql += " LIMIT ?"
            cur.execute(sql, (limit,))
        else:
            cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def get_rows_for(cur, file_unique_id: str):
    cur.execute(
        """
        SELECT chat_id, message_id, file_id, file_unique_id, nombre
        FROM videos_telegram
        WHERE file_unique_id = ?
        ORDER BY chat_id, message_id
        """,
        (file_unique_id,),
    )
    return cur.fetchall()


def main(limit: int | None = None, show_rows: bool = False, limit_chats: int | None = None, by_file_id: bool = False):
    db_path = Path(DB_PATH)
    if not db_path.exists():
        raise SystemExit(f"No existe la BD en {db_path}")

    dupes = get_duplicates_by_file_id(limit) if by_file_id else get_duplicates(limit)
    total = len(dupes)
    label = "file_id" if by_file_id else "file_unique_id"
    print(f"Duplicados por {label}: {total}")
    if dupes:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.cursor()
            for file_unique_id, count in dupes:
                print(f"\n{file_unique_id} → {count} filas")
                if show_rows:
                    rows = get_rows_for(cur, file_unique_id)
                    for r in rows:
                        chat_id, message_id, file_id, fu_id, nombre = r
                        print(f"  chat={chat_id} msg={message_id} file_id={file_id} nombre={nombre}")
        finally:
            conn.close()

    chats = get_chats_more_than_indexed(limit_chats)
    total_chats = len(chats)
    print(f"\nCanales con videos_count > indexados: {total_chats}")
    for chat_id, videos_count, indexados, duplicados, faltantes in chats:
        print(
            f"chat={chat_id} videos_count={videos_count} indexados={indexados} "
            f"duplicados={duplicados} faltantes={faltantes}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Obtiene todos los file_unique_id duplicados en videos_telegram."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limitar cantidad de file_unique_id duplicados mostrados (default: todos).",
    )
    parser.add_argument(
        "--show-rows",
        action="store_true",
        help="Muestra las filas (chat_id/message_id/file_id) de cada duplicado.",
    )
    parser.add_argument(
        "--limit-chats",
        type=int,
        default=None,
        help="Limitar cantidad de chats a listar con videos_count > indexados (default: todos).",
    )
    parser.add_argument(
        "--by-file-id",
        action="store_true",
        help="Detecta duplicados agrupando por file_id en lugar de file_unique_id.",
    )
    args = parser.parse_args()
    main(
        limit=args.limit,
        show_rows=args.show_rows,
        limit_chats=args.limit_chats,
        by_file_id=args.by_file_id,
    )
