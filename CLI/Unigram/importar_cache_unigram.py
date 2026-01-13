import json
import sqlite3
from pathlib import Path
from typing import Iterable, List, Dict, Any

BASE_DIR = Path(__file__).resolve().parent.parent
JSON_PATH = BASE_DIR / "indice_maestro_cache.json"
DB_PATH = BASE_DIR / "database" / "unigram.db"


def leer_json(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalizar_fila(item: Dict[str, Any]) -> tuple:
    return (
        item.get("archivo"),
        item.get("tipo"),
        item.get("fecha_escaneo"),
        int(bool(item.get("encontrado"))),
        item.get("msg_id_global"),
    )


def insertar_en_db(rows: Iterable[tuple]) -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO cacheo
            (archivo, tipo, fecha_escaneo, encontrado, msg_id_global)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def main():
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"No se encontró el JSON en {JSON_PATH}")
    if not DB_PATH.exists():
        raise FileNotFoundError(f"No se encontró la BD en {DB_PATH}")

    data = leer_json(JSON_PATH)
    rows = [normalizar_fila(item) for item in data]
    insertados = insertar_en_db(rows)
    print(f"✅ Insertados {insertados} registros en cacheo (ignorando duplicados por archivo).")


if __name__ == "__main__":
    main()
