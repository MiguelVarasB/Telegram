import os
import sqlite3
from pathlib import Path

from config import DB_PATH
from unigram_cacheo.common import CARPETAS, DB_LOCAL, preparar_base_local


HAS_CACHE_VAL = 5


def ensure_has_cache_column(cur_main: sqlite3.Cursor) -> None:
    cur_main.execute("PRAGMA table_info(videos_telegram)")
    cols = {row[1] for row in cur_main.fetchall()}
    if "has_cache" not in cols:
        cur_main.execute("ALTER TABLE videos_telegram ADD COLUMN has_cache INTEGER")


def run():
    # Abrir bases
    conn_local, _ = preparar_base_local()  # usa database/unigram.db
    conn_main = sqlite3.connect(DB_PATH)

    try:
        cur_main = conn_main.cursor()
        ensure_has_cache_column(cur_main)

        cur_local = conn_local.cursor()
        filas = cur_local.execute(
            "SELECT archivo, unique_id FROM cacheo WHERE tipo = 'video' AND unique_id IS NOT NULL"
        ).fetchall()

        total = len(filas)
        hallados = 0
        marcados = 0
        faltantes = 0

        print(f"🔎 Revisando {total} registros de video en unigram.db…")

        for idx, (archivo, unique_id) in enumerate(filas, start=1):
            if idx % 200 == 0 or idx == total:
                print(f"  Procesando {idx}/{total}…")

            ruta = Path(CARPETAS.get("video", "")) / archivo
            if ruta.exists():
                hallados += 1
                cur_main.execute(
                    "UPDATE videos_telegram SET has_cache = ? WHERE file_unique_id = ?",
                    (HAS_CACHE_VAL, unique_id),
                )
                marcados += cur_main.rowcount
            else:
                print(f"❌ Archivo faltante: {ruta}")
                faltantes += 1

        conn_main.commit()
        print("\nResumen cacheo → has_cache=5:")
        print(f"  Total registros considerados: {total}")
        print(f"  Archivos encontrados en disco: {hallados}")
        print(f"  Marcados en videos_telegram:  {marcados}")
        print(f"  Archivos faltantes en disco:  {faltantes}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn_local.close()
        conn_main.close()


if __name__ == "__main__":
    run()
