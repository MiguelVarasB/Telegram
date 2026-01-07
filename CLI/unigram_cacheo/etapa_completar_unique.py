import sqlite3
from sqlcipher3 import dbapi2 as sqlcipher

from config import DB_PATH as MAIN_DB_PATH
from .common import preparar_base_local, MASTER_KEY, DB_UNIGRAM

from utils import  log_timing
def _buscar_en_unigram_por_nombre(nombre_archivo: str, cur_unigram) -> tuple[int | None, int | None]:
    """
    Busca en la base cifrada de Unigram (messages.data) un registro cuyo blob contenga el nombre del archivo.
    Devuelve (chat_id, msg_id_real) o (None, None) si no se encuentra.
    """
    nombre_bytes = nombre_archivo.encode("utf-8")
    cur_unigram.execute("SELECT dialog_id, message_id, data FROM messages WHERE data IS NOT NULL")
    for d_id, m_id, blob in cur_unigram:
        if nombre_bytes in blob:
            msg_id_real = m_id // 1048576
            return d_id, msg_id_real
    return None, None


def completar_unique_ids():
    conn_local, _ = preparar_base_local()
    try:
        # Paso previo: llenar canal_id/msg_id_global para videos sin mapeo usando búsqueda directa en Unigram
        try:
            cur_local = conn_local.cursor()
            pendientes_sin_mapa = cur_local.execute(
                "SELECT id, archivo FROM cacheo WHERE tipo = 'video' AND canal_id IS NULL AND msg_id_global IS NULL"
            ).fetchall()
            if pendientes_sin_mapa:
                log_timing(f"🔎 Buscando canal/msg en db.sqlite para {len(pendientes_sin_mapa)} videos sin mapeo...")
                conn_uni = sqlcipher.connect(DB_UNIGRAM)
                cur_uni = conn_uni.cursor()
                cur_uni.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
                cur_uni.execute("PRAGMA cipher_compatibility = 4;")

                # Prepara mapa de nombres -> ids para un solo barrido de messages (evita 1 scan por archivo)
                pendientes_map = {}
                for cid_cacheo, nombre in pendientes_sin_mapa:
                    nb = nombre.encode("utf-8")
                    pendientes_map.setdefault(nb, []).append((cid_cacheo, nombre))

                hallados = 0
                procesados_msg = 0
                cur_uni.execute("SELECT dialog_id, message_id, data FROM messages WHERE data IS NOT NULL")
                for d_id, m_id, blob in cur_uni:
                    procesados_msg += 1
                    # Probar cada nombre pendiente en este blob (128 máx, se reduce al encontrar)
                    for nb, lista_ids in list(pendientes_map.items()):
                        if nb in blob:
                            msg_id_real = m_id // 1048576
                            for cid_cacheo, _nombre in lista_ids:
                                cur_local.execute(
                                    "UPDATE cacheo SET canal_id = ?, msg_id_global = ? WHERE id = ?",
                                    (d_id, msg_id_real, cid_cacheo),
                                )
                                hallados += 1
                            pendientes_map.pop(nb, None)
                    if procesados_msg % 500 == 0:
                        log_timing(f"  🧭 Escaneados {procesados_msg} mensajes; hallados {hallados}/{len(pendientes_sin_mapa)}")
                    if not pendientes_map:
                        break
                # Log final de este paso
                log_timing(f"  🔎 Mapeo terminado. Mensajes leídos: {procesados_msg}. Hallados: {hallados}/{len(pendientes_sin_mapa)}")
                if hallados:
                    conn_local.commit()
                conn_uni.close()
        except Exception as e:
            log_timing(f"⚠️ No se pudo completar canal/msg desde Unigram: {e}")

        log_timing("🔗 Buscando unique_id en base principal...")
        completados = 0
        main_db = sqlite3.connect(MAIN_DB_PATH)
        try:
            cur_main = main_db.cursor()
            cur_local = conn_local.cursor()
            cur_update = conn_local.cursor()

            pendientes_unique = cur_local.execute(
                "SELECT COUNT(*) FROM cacheo WHERE unique_id IS NULL AND canal_id IS NOT NULL AND msg_id_global IS NOT NULL"
            ).fetchone()[0]
            if pendientes_unique:
                log_timing(f"  Pendientes por completar unique_id: {pendientes_unique}")

            rows_pendientes = cur_local.execute(
                "SELECT id, canal_id, msg_id_global FROM cacheo WHERE unique_id IS NULL AND canal_id IS NOT NULL AND msg_id_global IS NOT NULL"
            ).fetchall()

            for idx, row in enumerate(rows_pendientes, start=1):
                cid = row[1]
                mid = row[2]
                if idx % 10 == 0 or idx == pendientes_unique:
                    log_timing(f"  🔎 Buscando unique_id {idx}/{pendientes_unique} (chat {cid}, msg {mid})")

                cur_main.execute(
                    "SELECT video_id FROM video_messages WHERE chat_id = ? AND message_id = ?",
                    (cid, mid),
                )
                r = cur_main.fetchone()
                valor_unique = r[0] if r and r[0] else None

                if not valor_unique:
                    cur_main.execute(
                        "SELECT file_unique_id FROM videos_telegram WHERE chat_id = ? AND message_id = ?",
                        (cid, mid),
                    )
                    r2 = cur_main.fetchone()
                    valor_unique = r2[0] if r2 and r2[0] else None

                if valor_unique:
                    cur_update.execute(
                        "UPDATE cacheo SET unique_id = ? WHERE id = ?",
                        (valor_unique, row[0]),
                    )
                    completados += 1

            if completados:
                conn_local.commit()
            log_timing(f"🔍 unique_id actualizados: {completados}")
        finally:
            main_db.close()
    except Exception as e:
        log_timing(f"⚠️ No se pudieron completar unique_id: {e}")
    finally:
        conn_local.close()


if __name__ == "__main__":
    completar_unique_ids()
