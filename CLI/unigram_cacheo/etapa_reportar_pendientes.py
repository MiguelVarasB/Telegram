import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import THUMB_FOLDER, DB_PATH
from utils import save_image_as_webp, log_timing
from .common import preparar_base_local

def _copiar_a_webp(origen, destino) -> bool:
    try:
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        save_image_as_webp(str(origen), str(destino))
        return True
    except Exception:
        return False


def _marcar_en_servidor(cur_local: sqlite3.Cursor, cacheo_id: int, valor: int) -> None:
    cur_local.execute("UPDATE cacheo SET en_servidor = ? WHERE id = ?", (valor, cacheo_id))


def _marcar_has_thumb_main(cur_main: sqlite3.Cursor, chat_id: int, msg_id: int, valor: int) -> None:
    cur_main.execute(
        "UPDATE videos_telegram SET has_thumb = ?, thumb_phash = '' WHERE chat_id = ? AND message_id = ?",
        (valor, chat_id, msg_id),
    )


def reportar_thumbs_pendientes(limit: int = 20):
    conn_local, _ = preparar_base_local()
    main_db = sqlite3.connect(DB_PATH)
    try:
        cur_local = conn_local.cursor()
        cur_main = main_db.cursor()

        # Resetear estado para reintentar todos los thumbnails
     ####  cur_local.execute("UPDATE cacheo SET en_servidor = 0 WHERE tipo = 'thumbnail'")

        # Traer pendientes con datos mínimos para ubicar en principal
        pendientes = cur_local.execute(
            """
            SELECT id, archivo, canal_id, msg_id_global
            FROM cacheo
            WHERE tipo = 'thumbnail' AND (en_servidor IS NULL OR en_servidor = 0)
            """
        ).fetchall()
        log_timing(f"🖼️ Thumbs pendientes de enviar al servidor: {len(pendientes)}")

        # Preparar trabajos (consulta DB y búsqueda de origen siguen serial para evitar múltiples conexiones)
        from .common import CARPETAS
        trabajos = []
        saltados = 0

        for idx, (cacheo_id, archivo, canal_id, msg_id) in enumerate(pendientes, start=1):
            if idx % 200 == 0:
                log_timing(f"  Preparando {idx}/{len(pendientes)}: {archivo}")

            if canal_id is None or msg_id is None:
                _marcar_en_servidor(cur_local, cacheo_id, -1)
                saltados += 1
                continue

            cur_main.execute(
                "SELECT has_thumb, file_unique_id FROM videos_telegram WHERE chat_id = ? AND message_id = ?",
                (canal_id, msg_id),
            )
            row = cur_main.fetchone()
            if not row:
                _marcar_en_servidor(cur_local, cacheo_id, -1)
                saltados += 1
                continue

            has_thumb, file_unique_id = row

            # Si ya tiene thumb marcado (2 o 3), no se recrea: solo marcar en_servidor=3
            if has_thumb and has_thumb > 1:
                _marcar_en_servidor(cur_local, cacheo_id, 3)
                saltados += 1
                continue

            if not file_unique_id:
                _marcar_en_servidor(cur_local, cacheo_id, -1)
                saltados += 1
                continue

            origen = None
            for carpeta in (CARPETAS.get("thumbnail"), CARPETAS.get("video")):
                if not carpeta:
                    continue
                ruta = Path(carpeta) / archivo
                if ruta.exists():
                    origen = ruta
                    break

            if origen is None or not origen.exists():
                _marcar_en_servidor(cur_local, cacheo_id, -1)
                saltados += 1
                continue

            destino = Path(THUMB_FOLDER) / str(canal_id) / f"{file_unique_id}.webp"
            trabajos.append(
                {
                    "cacheo_id": cacheo_id,
                    "canal_id": canal_id,
                    "msg_id": msg_id,
                    "has_thumb": has_thumb,
                    "origen": origen,
                    "destino": destino,
                }
            )

        # Ejecutar en paralelo la copia/convertido de thumbs
        max_workers = min(12, (os.cpu_count() or 4))
        log_timing(f"🚀 Lanzando {len(trabajos)} trabajos en paralelo (workers={max_workers})")

        resultados = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(_copiar_a_webp, job["origen"], job["destino"]): job for job in trabajos
            }
            for idx, future in enumerate(as_completed(future_to_job), start=1):
                job = future_to_job[future]
                ok = False
                try:
                    ok = future.result()
                except Exception:
                    ok = False
                resultados.append((job, ok))
                if idx % 200 == 0 or idx == len(trabajos):
                    log_timing(f"  ✅ Procesados {idx}/{len(trabajos)} trabajos (paralelo)")

        # Aplicar resultados a las bases
        for job, ok in resultados:
            cacheo_id = job["cacheo_id"]
            canal_id = job["canal_id"]
            msg_id = job["msg_id"]
            has_thumb = job["has_thumb"]
            if ok:
                _marcar_en_servidor(cur_local, cacheo_id, 2)
                if has_thumb == 0:
                    _marcar_has_thumb_main(cur_main, canal_id, msg_id, 5)
                else:
                    _marcar_has_thumb_main(cur_main, canal_id, msg_id, 6)
            else:
                _marcar_en_servidor(cur_local, cacheo_id, -1)

        conn_local.commit()
        main_db.commit()

        # Reporte final
        pendientes_servidor = cur_local.execute(
            "SELECT COUNT(*) FROM cacheo WHERE tipo = 'thumbnail' AND (en_servidor IS NULL OR en_servidor = 0)"
        ).fetchone()[0]
        log_timing(f"Pendientes tras proceso: {pendientes_servidor}")

        if pendientes_servidor:
            ejemplos = cur_local.execute(
                "SELECT archivo FROM cacheo WHERE tipo = 'thumbnail' AND (en_servidor IS NULL OR en_servidor = 0) LIMIT ?",
                (limit,),
            ).fetchall()
            log_timing("  Ejemplos pendientes:")
            for row in ejemplos:
                log_timing(f"   - {row[0]}")
            if pendientes_servidor > limit:
                log_timing("  ...")
    except Exception as e:
        log_timing(f"⚠️ Error en reporte/proceso de thumbs pendientes: {e}")
    finally:
        conn_local.close()
        main_db.close()


if __name__ == "__main__":
    reportar_thumbs_pendientes()
