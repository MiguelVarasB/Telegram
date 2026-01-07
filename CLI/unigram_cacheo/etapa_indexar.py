import os
import struct
import re
import datetime
from typing import Iterable

from sqlcipher3 import dbapi2 as sqlcipher

from .common import (
    CARPETAS,
    MASTER_KEY,
    DB_UNIGRAM,
    preparar_base_local,
    obtener_duracion_video,
    cargar_mensajes_unigram,
)
from utils import  log_timing

def iter_archivos_nuevos(archivos_ya_indexados: set) -> list[dict]:
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    lista = []
    for tipo, ruta in CARPETAS.items():
        if not os.path.exists(ruta):
            continue
        for f in os.listdir(ruta):
            if f.lower().endswith(extensiones) or f.isdigit():
                if f not in archivos_ya_indexados:
                    full_path = os.path.join(ruta, f)
                    lista.append(
                        {
                            "nombre": f,
                            "tipo": tipo,
                            "ruta": full_path,
                            "fecha_creacion": os.path.getctime(full_path),
                        }
                    )
    lista.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return lista


def _buscar_en_unigram_por_nombre(nombre_archivo: str, cur_unigram) -> tuple[int | None, int | None]:
    nombre_bytes = nombre_archivo.encode("utf-8")
    cur_unigram.execute("SELECT dialog_id, message_id, data FROM messages WHERE data IS NOT NULL")
    for d_id, m_id, blob in cur_unigram:
        if nombre_bytes in blob:
            msg_id_real = m_id // 1048576
            return d_id, msg_id_real
    return None, None


def procesar_archivos(lista_a_procesar: Iterable[dict]) -> None:
    conn_local, archivos_ya_indexados = preparar_base_local()
    nuevos_hallazgos = 0
    batch_size = 100
    pendientes = 0
    saltados_patron: list[str] = []

    # Cargar mensajes cifrados de Unigram
    conn_unigram, todos_los_mensajes = cargar_mensajes_unigram()
    log_timing(f"✅ {len(todos_los_mensajes)} mensajes cargados.")

    # Conexión para fallback por nombre
    conn_uni_nom = sqlcipher.connect(DB_UNIGRAM)
    cur_uni_nom = conn_uni_nom.cursor()
    cur_uni_nom.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
    cur_uni_nom.execute("PRAGMA cipher_compatibility = 4;")

    try:
        cur_local = conn_local.cursor()
        total_items = len(lista_a_procesar)
        for idx, item in enumerate(lista_a_procesar, start=1):
            nombre_f = item["nombre"]
            if idx % 50 == 0 or idx == total_items:
                log_timing(f"🔎 Procesando {idx}/{total_items}: {nombre_f}")

            match = re.search(r"(\d{15,20})", nombre_f)
            if not match:
                # Para videos sin número, igual guardamos tamaño y duración
                if item["tipo"] == "video":
                    tamano_bytes = os.path.getsize(item["ruta"])
                    duracion_segundos = obtener_duracion_video(item["ruta"])
                    cur_local.execute(
                        """
                        INSERT OR IGNORE INTO cacheo
                        (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            nombre_f,
                            item["tipo"],
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            0,
                            None,
                            None,
                            tamano_bytes,
                            duracion_segundos,
                            0,
                            None,
                        ),
                    )
                    pendientes += 1
                    nuevos_hallazgos += 1
                    if pendientes >= batch_size:
                        conn_local.commit()
                        log_timing(f"💾 Guardados {nuevos_hallazgos} registros (commit parcial)...")
                        pendientes = 0
                else:
                    saltados_patron.append(nombre_f)
                continue

            id_cache_num = int(match.group(1))
            huella_bin = struct.pack("<q", id_cache_num)

            tamano_bytes = os.path.getsize(item["ruta"])
            duracion_segundos = None
            if item["tipo"] == "video":
                duracion_segundos = obtener_duracion_video(item["ruta"])

            info = {
                "archivo": nombre_f,
                "tipo": item["tipo"],
                "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "encontrado": 0,
                "canal_id": None,
                "msg_id_global": None,
                "tamano_bytes": tamano_bytes,
                "duracion_segundos": duracion_segundos,
            }

            for m_id, d_id, data_blob in todos_los_mensajes:
                if data_blob and huella_bin in data_blob:
                    info["encontrado"] = 1
                    info["canal_id"] = d_id
                    info["msg_id_global"] = m_id // 1048576
                    break

            cur_local.execute(
                """
                INSERT OR IGNORE INTO cacheo
                (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    info["archivo"],
                    info["tipo"],
                    info["fecha_escaneo"],
                    info["encontrado"],
                    info["canal_id"],
                    info["msg_id_global"],
                    info["tamano_bytes"],
                    info["duracion_segundos"],
                    0,
                    None,
                ),
            )
            pendientes += 1
            nuevos_hallazgos += 1

            if pendientes >= batch_size:
                conn_local.commit()
                log_timing(f"💾 Guardados {nuevos_hallazgos} registros (commit parcial)...")
                pendientes = 0

            if info["encontrado"]:
                log_timing(
                    f"  ✅ {info['tipo'][:3].upper()} {nombre_f} -> Global: {info['msg_id_global']} (Canal: {info['canal_id']})"
                )
            # Fallback: si es video y no se halló por blob, intentar por nombre directo en db.sqlite
            if info["tipo"] == "video" and not info["encontrado"]:
                chat_id_f, msg_id_f = _buscar_en_unigram_por_nombre(nombre_f, cur_uni_nom)
                if chat_id_f is not None and msg_id_f is not None:
                    cur_local.execute(
                        """
                        UPDATE cacheo
                        SET encontrado = 1, canal_id = ?, msg_id_global = ?
                        WHERE archivo = ?
                        """,
                        (chat_id_f, msg_id_f, info["archivo"]),
                    )
                    info["encontrado"] = 1
                    info["canal_id"] = chat_id_f
                    info["msg_id_global"] = msg_id_f
                    log_timing(f"  ✅ Fallback nombre -> Global: {msg_id_f} (Canal: {chat_id_f}) para {nombre_f}")

        if pendientes:
            conn_local.commit()

        log_timing(f"\n✨ ¡Hecho! Se agregaron {nuevos_hallazgos} entradas a cacheo (ignorando duplicados).")
        if saltados_patron:
            log_timing(f"⚠️ Saltados por no cumplir patrón (sin número 15-20 dígitos): {len(saltados_patron)}")
            for name in saltados_patron[:30]:
                log_timing(f"   - {name}")
            if len(saltados_patron) > 30:
                log_timing("   ...")
    finally:
        conn_local.close()
        conn_unigram.close()
        conn_uni_nom.close()


def run_etapa_indexar():
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    lista = iter_archivos_nuevos(existentes)
    if not lista:
        log_timing("☕ No hay archivos nuevos que procesar.")
        return
    log_timing(f"🚀 Analizando {len(lista)} archivos nuevos...")
    procesar_archivos(lista)


if __name__ == "__main__":
    run_etapa_indexar()
