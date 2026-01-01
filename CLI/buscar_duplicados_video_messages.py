import argparse
import sqlite3
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH  # noqa: E402

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def encontrar_duplicados(limit: int):
    """Devuelve lista de (video_id, total) con total > 1."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT video_id, COUNT(*) AS total
            FROM video_messages
            GROUP BY video_id
            HAVING total > 1
            ORDER BY total DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()

def obtener_mensajes(video_id: str):
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT chat_id, message_id, date, views, forwards, caption
            FROM video_messages
            WHERE video_id = ?
            ORDER BY date
            """,
            (video_id,),
        )
        return cur.fetchall()

def main():
    parser = argparse.ArgumentParser(
        description="Busca video_id duplicados en la tabla video_messages."
    )
    parser.add_argument("--limit", type=int, default=987654321, help="Máximo de duplicados a listar")
    parser.add_argument(
        "--detallar", action="store_true", help="Mostrar los mensajes asociados a cada video duplicado",
    )
    args = parser.parse_args()

    db_path = Path(DB_PATH)
    if not db_path.exists():
        print(f"No existe la BD en {db_path}")
        raise SystemExit(1)

    duplicados = encontrar_duplicados(args.limit)
    if not duplicados:
        print("No se encontraron duplicados en video_messages.")
        return

    print(f"Encontrados {len(duplicados)} video_id con más de una referencia (limit {args.limit}).")
    for row in duplicados:
        vid = row["video_id"]
        total = row["total"]
        print(f"\nvideo_id={vid} -> {total} mensajes")
        if args.detallar:
            mensajes = obtener_mensajes(vid)
            for m in mensajes:
                print(
                    f"  chat={m['chat_id']} msg={m['message_id']} date={m['date']} "
                    f"views={m['views']} fwd={m['forwards']} caption={ (m['caption'] or '')[:80]}"
                )

if __name__ == "__main__":
    main()
