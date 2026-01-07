import argparse
import sqlite3
from pathlib import Path
from typing import List, Tuple
from utils import log_timing

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database" / "unigram.db"


def obtener_sin_unique_id(limit: int) -> List[Tuple]:
    """
    Devuelve registros de cacheo cuyo unique_id sea NULL (no debería haber, pero revisamos).
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"No se encontró la BD en {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT unique_id, archivo, msg_id_global, tipo, fecha_escaneo, encontrado
            FROM cacheo
            WHERE unique_id IS NULL
            LIMIT ?
            """
            ,
            (limit,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Recorre cacheo y muestra filas sin unique_id (debería ser vacío)."
    )
    parser.add_argument(
        "--limit", type=int, default=100, help="Cantidad máxima de filas a mostrar (default 100)"
    )
    args = parser.parse_args()

    filas = obtener_sin_unique_id(args.limit)
    if not filas:
        log_timing("✅ No hay registros sin unique_id (todo correcto).")
        return

    log_timing(f"⚠️ Se encontraron {len(filas)} filas sin unique_id:")
    for unique_id, archivo, msg_id_global, tipo, fecha_escaneo, encontrado in filas:
        log_timing(
            f"unique_id={unique_id}, archivo={archivo}, msg_id_global={msg_id_global}, "
            f"tipo={tipo}, fecha_escaneo={fecha_escaneo}, encontrado={encontrado}"
        )


if __name__ == "__main__":
    main()
