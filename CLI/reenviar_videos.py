import os
import asyncio
import aiosqlite
import sys
import random
from pyrogram.errors import FloodWait
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importamos tu configuración
from config import (
    DB_PATH,
    API_ID,
    API_HASH,
    SESSION_NAME,
    CACHE_DUMP_VIDEOS_CHANNEL_ID,
    FOLDER_SESSIONS,
    CANALES_CON_ACCESO_FREE,
)
from services.telegram_client import get_client
from utils import save_image_as_webp, log_timing
from utils.database_helpers import ensure_column

# --- CONFIGURACIÓN ---
LIMITE =10000  # Cantidad de videos a procesar en esta vuelta
BATCH =   30  # Tamaño del paquete de reenvío (No subir de 30)
SLEEP_ENVIO = (3, 10)  # rango de espera aleatoria entre envíos
MAX_CHATS_CONCURRENTES = 3  # cuántos chats se procesan en paralelo

async def check_database_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_column(db, "videos_telegram", "dump_fail", "INTEGER", "0")

async def marcar_fallidos(video_ids, razon):
    if not video_ids:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join(["?"] * len(video_ids))
        await db.execute(
            f"UPDATE videos_telegram SET dump_fail = 1 WHERE id IN ({placeholders})",
            tuple(video_ids),
        )
        await db.commit()
    log_timing(f"\n🗑️ Marcados {len(video_ids)} registros como dump_fail=1. Razón: {razon}")

async def marcar_chat_fallido(chat_id, razon):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE videos_telegram
            SET dump_fail = 1
            WHERE chat_id = ?

              AND has_thumb = 0
              AND dump_message_id IS NULL
              AND (dump_fail IS NULL OR dump_fail = 0)
            """,
            (chat_id,),
        )
        await db.commit()
    log_timing(f"\n🗑️ Marcado chat {chat_id} como NO reenviable (dump_fail=1). Razón: {razon}")

async def main():
    if not CACHE_DUMP_VIDEOS_CHANNEL_ID:
        log_timing("❌ ERROR: Configura el CACHE_DUMP_VIDEOS_CHANNEL_ID en config.py")
        return {
            "pendientes": 0,
            "reenviados": 0,
        }

    log_timing(f"🚀 Iniciando reenvío de hasta {LIMITE} videos al canal {CACHE_DUMP_VIDEOS_CHANNEL_ID}...")

    await check_database_schema()

    # 1. Buscar videos pendientes en la BD
    exclude_chats = list(CANALES_CON_ACCESO_FREE or [])
    exclude_clause = ""
    params = []
    if exclude_chats:
        placeholders = ",".join(["?"] * len(exclude_chats))
        exclude_clause = f" AND chat_id NOT IN ({placeholders}) "
        params.extend(exclude_chats)

    params.append(LIMITE)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"""
            SELECT id, chat_id, message_id 
            FROM videos_telegram 
            WHERE has_thumb = 0 AND dump_message_id IS NULL 
              AND (dump_fail IS NULL OR dump_fail = 0)
            and chat_id!=5490529645
            {exclude_clause}
            LIMIT ?
        """,
            tuple(params),
        ) as cursor:
            pendientes = await cursor.fetchall()

    if not pendientes:
        log_timing("✅ No hay videos pendientes.")
        return {
            "pendientes": 0,
            "reenviados": 0,
        }

    log_timing(f"📦 Encontrados {len(pendientes)} videos. Conectando Userbot...")

    # 2. Conectar Userbot usando una sesión dedicada para este script
    app = get_client(custom_session_name="Reenvio_DUMP")

    if not app.is_connected:
        await app.start()

    # 3. Agrupar por chat de origen (Telegram requiere reenviar por chat)
    lotes: Dict[int, List[Tuple[int, int, int]]] = {}

    for vid in pendientes:
        chat_id = vid[1]
        if chat_id not in lotes: lotes[chat_id] = []
        lotes[chat_id].append(vid)

    total_ok = 0

    async def procesar_chat(chat_origin: int, videos: List[Tuple[int, int, int]]):
        nonlocal total_ok
        chat_bloqueado = False

        for i in range(0, len(videos), BATCH):
            chunk = videos[i : i + BATCH]
            msg_ids = [v[2] for v in chunk]  # Solo los IDs de los mensajes

            try:
                log_timing(f"➡️ Reenviando {len(chunk)} videos del chat {chat_origin}...")

                # REENVÍO
                nuevos_msgs = await app.forward_messages(
                    chat_id=CACHE_DUMP_VIDEOS_CHANNEL_ID,
                    from_chat_id=chat_origin,
                    message_ids=msg_ids
                )

                if not isinstance(nuevos_msgs, list):
                    nuevos_msgs = [nuevos_msgs] if nuevos_msgs else []

                if not nuevos_msgs:
                    log_timing(f"⚠️ Sin reenviar (lista vacía) para chat {chat_origin}")
                    continue

                # ACTUALIZAR BD
                async with aiosqlite.connect(DB_PATH) as db:
                    for j, msg in enumerate(nuevos_msgs):
                        if msg:
                            # Guardamos la referencia: video_id -> nuevo_message_id
                            vid_id_local = chunk[j][0]
                            await db.execute(
                                "UPDATE videos_telegram SET dump_message_id = ? WHERE id = ?", 
                                (msg.id, vid_id_local)
                            )
                    await db.commit()

                total_ok += len(nuevos_msgs)
                log_timing(f"✅ Reenviados {len(nuevos_msgs)} del chat {chat_origin}")
                await asyncio.sleep(random.uniform(*SLEEP_ENVIO))  # Pausa de seguridad

            except FloodWait as e:
                log_timing(f"\n⏳ FloodWait: Esperando {e.value} segundos...")
                await asyncio.sleep(e.value)
            except Exception as e:
                err_id = getattr(e, "ID", "")
                err_txt = str(e)
                if err_id == "CHAT_FORWARDS_RESTRICTED" or "CHAT_FORWARDS_RESTRICTED" in err_txt:
                    log_timing(f"\n❌ Error: {e}")
                    await marcar_chat_fallido(chat_origin, "CHAT_FORWARDS_RESTRICTED")
                    chat_bloqueado = True
                    break
                log_timing(f"\n❌ Error: {e}")

        return chat_bloqueado

    try:
        sem = asyncio.Semaphore(MAX_CHATS_CONCURRENTES)

        async def worker(chat_origin: int, videos: List[Tuple[int, int, int]]):
            async with sem:
                bloqueado = await procesar_chat(chat_origin, videos)
                return bloqueado

        tareas = [asyncio.create_task(worker(chat_origin, videos)) for chat_origin, videos in lotes.items()]
        resultados = await asyncio.gather(*tareas)
        # Si algún chat se bloqueó, ya quedó marcado en DB; no se necesita más manejo
    finally:
        await app.stop()

    log_timing(f"\n🏁 FIN. Total reenviados: {total_ok}")

    return {
        "pendientes": len(pendientes),
        "reenviados": total_ok,
    }

if __name__ == "__main__":
    asyncio.run(main())