import argparse
import asyncio
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Asegurar import desde la raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pyrogram import enums
from config import DB_PATH
from services.telegram_client import get_client
from services.video_processor import procesar_mensajes_video_batch
from database.connection import get_db, get_db_connection  # Conexión async optimizada
from utils import log_timing

# Configuración de logging para ver detalles
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Sincronizador")

# Configuración
BATCH_SIZE = 50

# Flag para no recrear índices en cada chat
_INDICES_VIDEO_MESSAGES_OK = False

DIAS_ATRAS=2

def _naive(dt):
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt

# ---------------------------------------------------------
# Helpers de Base de Datos (Ahora Asíncronos y Seguros)
# ---------------------------------------------------------
async def asegurar_indices_video_messages(db):
    """
    Garantiza índices necesarios para que la carga de IDs no haga full scan.
    Se ejecuta solo una vez por proceso.
    """
    global _INDICES_VIDEO_MESSAGES_OK
    if _INDICES_VIDEO_MESSAGES_OK:
        return
    await db.execute("CREATE INDEX IF NOT EXISTS idx_vm_chat_id ON video_messages(chat_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_vm_chat_msg ON video_messages(chat_id, message_id)")
    await db.commit()
    _INDICES_VIDEO_MESSAGES_OK = True

# ---------------------------------------------------------
# Helpers de Base de Datos (Ahora Asíncronos y Seguros)
# ---------------------------------------------------------
async def obtener_stats_chat(chat_id: int, db=None) -> tuple[int, int, int, int, int]:
    """
    stats del chat. Si se pasa db, reutiliza la misma conexión para evitar overhead.
    """
    if db is None:
        async with get_db() as db_ctx:
            return await obtener_stats_chat(chat_id, db_ctx)

    await asegurar_indices_video_messages(db)
    async with db.execute(
        """
        SELECT 
            COALESCE(indexados, 0),
            COALESCE(videos_count, 0) - COALESCE(duplicados, 0) AS total_unicos,
            COALESCE(videos_count, 0),
            COALESCE(duplicados, 0),
            (COALESCE(videos_count, 0) - COALESCE(indexados, 0)) AS faltantes
        FROM chat_video_counts
        WHERE chat_id = ?
        """,
        (chat_id,),
    ) as cur:
        row = await cur.fetchone()
        if not row: return 0, 0, 0, 0, 0
        return tuple(int(x or 0) for x in row)

async def obtener_chats_para_escanear(max_chats: int | None = None, only_chat_id: int | None = None):
    """
    Recupera los chats usando la conexión asíncrona para evitar bloqueos.
    """
    query = """
        SELECT cvc.chat_id
        FROM chat_video_counts cvc
        JOIN chats c ON cvc.chat_id = c.chat_id
        WHERE cvc.videos_count > 0
          AND c.activo = 1
          AND COALESCE(c.is_owner, 0) = 0
          AND (cvc.videos_count - COALESCE(cvc.indexados, 0)) > 0
        ORDER BY cvc.videos_count DESC
    """
    params = []
    if max_chats:
        query += " LIMIT ?"
        params.append(max_chats)

    async with get_db() as db:
        async with db.execute(query, tuple(params)) as cursor:
            rows = await cursor.fetchall()
            chats = [row[0] for row in rows]
    
    if only_chat_id is not None:
        chats = [cid for cid in chats if cid == only_chat_id]
        
    return chats

async def obtener_detalles_chats_para_filtrado(chat_ids: list[int]) -> dict[int, tuple[str, datetime | None, int, int]]:
    """
    Recupera nombre, last_message_date, indexados y faltantes para una lista de chat_ids.
    Retorna un diccionario: {chat_id: (name, last_message_date, indexados, faltantes)}
    """
    if not chat_ids:
        return {}

    query = f"""
        SELECT 
            c.chat_id, 
            c.name,
            c.last_message_date, 
            COALESCE(cvc.indexados, 0) as indexados,
            (COALESCE(cvc.videos_count, 0) - COALESCE(cvc.indexados, 0)) AS faltantes
        FROM chats c
        LEFT JOIN chat_video_counts cvc ON c.chat_id = cvc.chat_id
        WHERE c.chat_id IN ({','.join('?' for _ in chat_ids)})
    """
    
    detalles = {}
    async with get_db() as db:
        async with db.execute(query, tuple(chat_ids)) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                chat_id, name, last_date_str, indexados, faltantes = row
                last_date = datetime.fromisoformat(last_date_str) if last_date_str else None
                detalles[chat_id] = (name, last_date, indexados, faltantes)
    return detalles

# ---------------------------------------------------------
# WORKER: GRABADOR EN SEGUNDO PLANO
# ---------------------------------------------------------
async def worker_guardado(queue: asyncio.Queue):
    batch = []
    while True:
        try:
            msg = await queue.get()
            if msg is None:  # Señal de parada
                if batch: 
                    await procesar_mensajes_video_batch(batch, origen="sincronizador")
                queue.task_done()
                break
            
            batch.append(msg)
            if len(batch) >= BATCH_SIZE:
                try:
                    await procesar_mensajes_video_batch(batch, origen="sincronizador")
                except Exception as e:
                    logger.error(f"❌ Error guardando batch en background: {e}")
                finally:
                    batch.clear()
            queue.task_done()
        except Exception as e:
            logger.critical(f"❌ Error crítico en worker: {e}")

# ---------------------------------------------------------
# ESCÁNER PRINCIPAL (PRODUCTOR)
# ---------------------------------------------------------
async def escanear_chat_inteligente(
    client,
    chat_id: int,
    consecutivos_para_detener: int = 30,
    max_nuevos_por_chat: int | None = None,
    detalles_chats: dict[int, tuple[str, datetime | None, int, int]] | None = None,
):
    # Reusar conexión única para todo el flujo del chat
    db = await get_db_connection()
    # Stats iniciales
    indexados, total_unicos, videos_count, duplicados, faltantes = await obtener_stats_chat(chat_id, db)
    name, last_date = "?", None
    if detalles_chats and chat_id in detalles_chats:
        name, last_date, indexados, faltantes = detalles_chats[chat_id]
    last_date = _naive(last_date)
    max_fecha_mensajes = None
    log_timing(
        f"\n📺 Canal {name} | ID: {chat_id} | ultimo mensaje: {last_date} |Videos: {videos_count} | Indexados: {indexados} | "
        f"Duplicados: {duplicados} | Únicos: {total_unicos} | Faltan aprox: {faltantes}"
    )

    # 1. Cargar IDs a RAM (OPTIMIZADO CON WAL)
    log_timing("🧠 Cargando RAM (Conexión Async)...")
    start_ram = asyncio.get_running_loop().time()
    await asegurar_indices_video_messages(db)
    async with db.execute(
        "SELECT message_id FROM video_messages WHERE chat_id = ? ORDER BY message_id",
        (chat_id,),
    ) as cur:
        rows = await cur.fetchall()
        ids_locales = {row[0] for row in rows}
    end_ram = asyncio.get_running_loop().time()
    log_timing(f"✅ RAM Lista en {end_ram - start_ram:.2f}s: {len(ids_locales)} mensajes conocidos.")

    queue = asyncio.Queue()
    worker_task = asyncio.create_task(worker_guardado(queue))

    consecutivos_existentes = 0
    nuevos_detectados = 0
    mensajes_leidos_total = 0
    
    total_estimado = faltantes if faltantes > 0 else "?"
    
    try:
        log_timing("📡 Solicitando mensajes a Telegram (Modo flexible: Video + Documentos)...")
        
        # CAMBIO CLAVE: Usamos filtro EMPTY para ver todo y filtrar manualmente
        # Esto atrapa videos que fueron enviados como archivos.
        async for m in client.search_messages(chat_id): # Sin filtro estricto en la query inicial
            
            es_video_valido = False
            
            # Verificación manual de tipo
            if m.video:
                es_video_valido = True
            elif m.document:
                # Verificar si es un documento de video (mp4, mkv, avi, etc.)
                mime = (m.document.mime_type or "").lower()
                if "video" in mime or m.document.file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.mov')):
                    es_video_valido = True
                    # Log detallado para "Escaneo Fantasma"
                    # log_timing(f"  🔍 Documento detectado como video: {m.document.file_name}")

            if not es_video_valido:
                continue

            mensajes_leidos_total += 1
            nombre = (m.video.file_name if m.video else m.document.file_name) or "sin_nombre"

            # Registrar fecha más reciente si el chat no tiene last_message_date
            if last_date is None and getattr(m, "date", None):
                fecha_msg = _naive(m.date)
                if fecha_msg:
                    if max_fecha_mensajes is None or fecha_msg > max_fecha_mensajes:
                        max_fecha_mensajes = fecha_msg
            
            if m.id in ids_locales:
                consecutivos_existentes += 1
                log_timing(f"  ℹ️  Ya existe ({consecutivos_existentes}/{consecutivos_para_detener}): {nombre[:30]} | msg_id={m.id}")
                
                if consecutivos_existentes >= consecutivos_para_detener:
                    log_timing(f"🛑 Umbral de detención alcanzado ({consecutivos_existentes} existentes seguidos).")
                    break
            else:
                if consecutivos_existentes > 0:
                    log_timing(f"  🔄 Reset contador stop ({consecutivos_existentes}->0)")
                    consecutivos_existentes = 0
                
                nuevos_detectados += 1
                contador_str = f"{nuevos_detectados}/{total_estimado}"
                log_timing(f"  ✨ NUEVO ({contador_str}): {nombre[:40]} | msg_id={m.id}")
                
                queue.put_nowait(m)

                if max_nuevos_por_chat and nuevos_detectados >= max_nuevos_por_chat:
                    log_timing(f"✅ Límite de nuevos alcanzado ({max_nuevos_por_chat}).")
                    break
        
        if mensajes_leidos_total == 0:
             log_timing("⚠️ ALERTA: Telegram no devolvió ningún video/documento, pero el contador de BD dice que deberían existir. Posiblemente borrados o fuera del rango de búsqueda.")

    except Exception as e:
        logger.error(f"❌ Error en escaneo del chat {chat_id}: {e}", exc_info=True)
    
    finally:
        log_timing(f"⏳ Finalizando chat {chat_id}. Enviando señal de parada al worker...")
        await queue.put(None)
        log_timing(f"⏳ Esperando que el worker del chat {chat_id} guarde el último lote...")
        await worker_task
        log_timing(f"✅ Worker del chat {chat_id} finalizado.")
        log_timing(f"📊 Chat {chat_id} finalizado. {nuevos_detectados} nuevos guardados.")

        # Si el chat no tenía last_message_date, usar la fecha más reciente encontrada
        if last_date is None and max_fecha_mensajes is not None:
            try:
                await db.execute(
                    "UPDATE chats SET last_message_date = ? WHERE chat_id = ?",
                    (max_fecha_mensajes.isoformat(), chat_id),
                )
                await db.commit()
                log_timing(
                    f"🗓️ last_message_date actualizado para {chat_id} -> {max_fecha_mensajes.isoformat()}"
                )
            except Exception as e:
                log_timing(f"⚠️ No se pudo actualizar last_message_date para {chat_id}: {e}")
        await db.close()

# ---------------------------------------------------------
# INTERFAZ PÚBLICA
# ---------------------------------------------------------
async def sync_con_stop(
    max_chats: int | None = None,
    only_chat_id: int | None = None,
    consecutivos_para_detener: int = 30,
    max_nuevos_por_chat: int | None = None,
    concurrencia: int = 1,
):
    log_timing(f"-==== Iniciando sincronizacion con stop ====-")
    client = get_client(clone_for_cli=True)
    started_here = False
    if not client.is_connected:
        await client.start()
        started_here = True
    try:
        chats_iniciales = await obtener_chats_para_escanear(max_chats, only_chat_id)
        log_timing(f"🔎 Chats candidatos iniciales: {len(chats_iniciales)}")

        # --- Filtro Adicional --- #
        detalles_chats = await obtener_detalles_chats_para_filtrado(chats_iniciales)
        chats_filtrados = []
        dias_atras_dt = datetime.now() - timedelta(days=DIAS_ATRAS)

        for chat_id in chats_iniciales:
            detalles = detalles_chats.get(chat_id)
            if not detalles:
                chats_filtrados.append(chat_id)
                continue

            name, last_date, indexados, faltantes = detalles
            if faltantes is not None and faltantes <= 1:
                log_timing(
                    f"  -> Excluyendo chat '{name}' ({chat_id}): faltantes={faltantes} (<=1)."
                )
                continue
            
            # Condición de exclusión: si el último mensaje es antiguo Y ya hay suficientes indexados
            if (
                dias_atras_dt
                and last_date
                and last_date < dias_atras_dt
                and indexados > 30
                and faltantes > 0
            ):
                log_timing(
                    f"  -> Excluyendo chat '{name}' ({chat_id}): Sin actividad reciente ({last_date.strftime('%Y-%m-%d')}) y {indexados} indexados."
                )
                continue
            
            chats_filtrados.append(chat_id)
        
        chats = chats_filtrados
        log_timing(f"🚀 Iniciando revisión de {len(chats)} chats (concurrencia: {concurrencia})...")
        
        # Procesar en lotes concurrentes para evitar el delay de 11s entre chats
        for i in range(0, len(chats), concurrencia):
            batch = chats[i:i+concurrencia]
            tasks = [
                escanear_chat_inteligente(
                    client, 
                    cid, 
                    consecutivos_para_detener=consecutivos_para_detener,
                    max_nuevos_por_chat=max_nuevos_por_chat,
                    detalles_chats=detalles_chats
                )
                for cid in batch
            ]
            await asyncio.gather(*tasks)
    finally:
        if client.is_connected and started_here:
            log_timing("⏳ Desconectando cliente de Telegram (timeout: 15s)...")
            try:
                await asyncio.wait_for(client.stop(), timeout=15.0)
                log_timing("✅ Cliente de Telegram desconectado.")
            except asyncio.TimeoutError:
                log_timing("⚠️ Timeout al desconectar el cliente. Forzando salida.")
            except Exception as e:
                log_timing(f"❌ Error al desconectar cliente: {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-chats", type=int, default=None)
    parser.add_argument("--only-chat-id", type=int, default=None)
    parser.add_argument("--consecutivos", type=int, default=30)
    parser.add_argument("--max-nuevos-chat", type=int, default=None)
    parser.add_argument("--concurrencia", type=int, default=1, help="Número de chats a procesar en paralelo (aumentar con cuidado, puede causar flood wait)")
    args = parser.parse_args()

    await sync_con_stop(
        max_chats=args.max_chats,
        only_chat_id=args.only_chat_id,
        consecutivos_para_detener=args.consecutivos,
        max_nuevos_por_chat=args.max_nuevos_chat,
        concurrencia=args.concurrencia
    )

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(main())

    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        log_timing("\n🛑 Interrupción por teclado detectada. Cancelando tareas...")
        main_task.cancel()
        # Esperar a que las tareas se cancelen
        loop.run_until_complete(main_task)
    except Exception as e:
        logger.error(f"Error inesperado en el bucle principal: {e}", exc_info=True)
    finally:
        log_timing("\n👋 Script finalizado.")
        loop.close()