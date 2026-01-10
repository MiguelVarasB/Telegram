import argparse
import asyncio
from typing import List, Tuple

import aiosqlite

from config import DB_PATH
from utils.helpers import convertir_tamano


async def obtener_canales_ordenados(limit: int, min_videos: int) -> List[Tuple]:
    query = """
        SELECT
            c.chat_id,
            COALESCE(c.name, 'Sin Nombre') AS name,
            c.username,
            COUNT(v.id) AS total_videos,
            AVG(COALESCE(v.tamano_bytes, 0)) AS promedio_bytes,
            MIN(COALESCE(v.tamano_bytes, 0)) AS minimo_bytes,
            MAX(COALESCE(v.tamano_bytes, 0)) AS maximo_bytes
        FROM videos_telegram v
        LEFT JOIN chats c ON c.chat_id = v.chat_id
        GROUP BY c.chat_id
        HAVING COUNT(v.id) >= ?
        ORDER BY promedio_bytes ASC
        LIMIT ?
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (min_videos, limit)) as cursor:
            rows = await cursor.fetchall()
    return rows


def imprimir_tabla(rows: List[Tuple]) -> None:
    headers = [
        "Chat ID",
        "Nombre",
        "Username",
        "# Videos",
        "Promedio",
        "Mínimo",
        "Máximo",
    ]
    col_widths = [12, 32, 18, 10, 12, 12, 12]

    def fmt(text: str, width: int) -> str:
        return (text or "")[: width - 1].ljust(width)

    header_line = " ".join(fmt(h, w) for h, w in zip(headers, col_widths))
    print(header_line)
    print("-" * len(header_line))

    for r in rows:
        username = f"@{r['username']}" if r["username"] else ""
        line = " ".join(
            [
                fmt(str(r["chat_id"]), col_widths[0]),
                fmt(r["name"], col_widths[1]),
                fmt(username, col_widths[2]),
                fmt(str(r["total_videos"]), col_widths[3]),
                fmt(convertir_tamano(r["promedio_bytes"]), col_widths[4]),
                fmt(convertir_tamano(r["minimo_bytes"]), col_widths[5]),
                fmt(convertir_tamano(r["maximo_bytes"]), col_widths[6]),
            ]
        )
        print(line)


async def main():
    parser = argparse.ArgumentParser(
        description="Listar canales cuyos videos pesan menos (por promedio)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Cantidad de canales a mostrar, ordenados de menor a mayor promedio de tamaño.",
    )
    parser.add_argument(
        "--min-videos",
        type=int,
        default=5,
        help="Mínimo de videos que debe tener el canal para incluirlo en el listado.",
    )
    args = parser.parse_args()

    rows = await obtener_canales_ordenados(args.limit, args.min_videos)
    if not rows:
        print("No se encontraron canales con videos.")
        return

    imprimir_tabla(rows)


if __name__ == "__main__":
    asyncio.run(main())
