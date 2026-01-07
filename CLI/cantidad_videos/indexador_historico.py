import argparse
import asyncio
import os
import sys

import aiosqlite
from pyrogram.errors import FloodWait

"""Resumen: Recorre hacia atrás chats con videos ya indexados para recuperar históricos y marcar last_hist_scan en chat_video_counts."""

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH,CACHE_DUMP_VIDEOS_CHANNEL_ID  # noqa: E402
from services.telegram_client import get_client  # noqa: E402
from database.videos import (  # noqa: E402
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
)
from database.chats import db_get_chat  # noqa: E402

Excluir=[
CACHE_DUMP_VIDEOS_CHANNEL_ID,
-1002670762200 ,
-1002621670240

    ]
async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        return db

# ---------------------------------------------------------
# 1. OBTENER CHATS QUE YA TIENEN ALGO INDEXADO
# ---------------------------------------------------------
async def _ensure_last_hist_scan_column(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(chat_video_counts)") as cur:
        cols = [row[1] async for row in cur]
    if "last_hist_scan" not in cols:
        await db.execute(
            "ALTER TABLE chat_video_counts ADD COLUMN last_hist_scan TEXT DEFAULT NULL"
        )
        await db.commit()


async def obtener_chats_con_historial():
    """
    Solo nos interesan chats que ya tengan al menos 1 video,
    para poder buscar 'hacia atrás' desde ese video.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await _ensure_last_hist_scan_column(db)
        async with db.execute(
            """
            SELECT chat_id 
            FROM chat_video_counts 
            WHERE videos_count > 0
              AND (last_hist_scan IS NULL OR last_hist_scan <= datetime('now','-1 day'))
            """
        ) as cursor:
            rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def obtener_nombre_chat(chat_id: int, client) -> str:
    # Primero intentamos base de datos
    try:
        info = await db_get_chat(chat_id)
        if info and info.get("name"):
            return info["name"]
    except Exception:
        pass
    # Fallback a Telegram
    try:
        chat_info = await client.get_chat(chat_id)
        return chat_info.title or str(chat_id)
    except Exception:
        return str(chat_id)


# ---------------------------------------------------------
# 2. OBTENER EL ID MÁS ANTIGUO (EL ANCLA)
# ---------------------------------------------------------
async def obtener_id_mas_antiguo(chat_id):
    """
    Busca el message_id más pequeño registrado en la BD.
    Este será nuestro punto de partida para ir al pasado.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT MIN(message_id) as min_id
            FROM video_messages 
            WHERE chat_id = ?
            """,
            (chat_id,),
        ) as cursor:
            row = await cursor.fetchone()

    return row[0] if row and row[0] else None

# ---------------------------------------------------------
# 3. PROCESAMIENTO (Igual que el sincronizador pero protegido)
# ---------------------------------------------------------
async def procesar_video_historico(message):
    # PROTECCIÓN: Si el mensaje no tiene video (puede pasar en históricos raros), saltamos
    if not message.video:
        return False

    video = message.video
    unique_id = video.file_unique_id

    # Verificamos existencia sin abrir muchas conexiones si es posible,
    # pero mantenemos tu estructura actual para compatibilidad.
    async def _existe_video(file_unique_id: str) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM videos_telegram WHERE file_unique_id = ? LIMIT 1",
                (file_unique_id,),
            ) as cur:
                row = await cur.fetchone()
                return row is not None

    ya_existe = await _existe_video(unique_id)
    es_nuevo = not ya_existe

    # Datos para upsert de video
    video_data = {
        "file_unique_id": unique_id,
        "chat_id": message.chat.id,
        "message_id": message.id,
        "file_id": video.file_id,
        "nombre": video.file_name or f"vid_{message.id}.mp4", # Fallback nombre
        "caption": message.caption,
        "tamano_bytes": video.file_size,
        "fecha_mensaje": message.date.isoformat() if message.date else None,
        "duracion": video.duration or 0,
        "ancho": video.width or 0,
        "alto": video.height or 0,
        "mime_type": video.mime_type,
        "views": message.views or 0,
        "outgoing": message.outgoing,
    }
    
    try:
        await db_upsert_video(video_data)
        # Importante: Si es histórico, el origen es 'historico'
        await db_add_video_file_id(unique_id, video.file_id, unique_id, origen="historico")

        # Datos de mensaje
        from_user = getattr(message, "from_user", None)
        forward_from_chat = getattr(message, "forward_from_chat", None)
        message_data = {
            "video_id": unique_id,
            "chat_id": message.chat.id,
            "message_id": message.id,
            "date": message.date.isoformat() if message.date else None,
            "from_user": {
                "id": getattr(from_user, "id", None),
                "username": getattr(from_user, "username", None),
                "is_bot": getattr(from_user, "is_bot", None),
            } if from_user else {},
            "media": "video",
            "views": message.views or 0,
            "forwards": message.forwards or 0,
            "outgoing": message.outgoing,
            "reply_to_message_id": getattr(message, "reply_to_message_id", None),
            "forward_from_chat": {
                "id": getattr(forward_from_chat, "id", None),
                "title": getattr(forward_from_chat, "title", None),
            } if forward_from_chat else {},
            "forward_from_message_id": getattr(message, "forward_from_message_id", None),
            "forward_date": (
                message.forward_date.isoformat() if getattr(message, "forward_date", None) else None
            ),
            "caption": message.caption,
        }
        await db_upsert_video_message(message_data)
    except Exception as e:
        print(f"[ERR] Error guardando ID {message.id}: {e}")
        return False

    return es_nuevo

# ---------------------------------------------------------
# 4. BUCLE DE ESCANEO HISTÓRICO (HACIA ATRÁS)
# ---------------------------------------------------------
async def escanear_historia_antigua(client, chat_id: int):
    if chat_id in Excluir:
        print(f"Chat {chat_id}: excluido, se salta.")
        return

    chat_nombre = await obtener_nombre_chat(chat_id, client)

    # Paso 1: Obtener el ancla
    id_mas_antiguo = await obtener_id_mas_antiguo(chat_id)
    
    if not id_mas_antiguo:
        print(f"Chat {chat_id}: No hay datos previos. Usa el sincronizador normal primero.")
        return

    print(f"--- Indexando HISTORIA de {chat_id} ({chat_nombre}) ---")
    print(f"--- Punto de partida (hacia atrás): ID {id_mas_antiguo} ---")

    count_nuevos = 0
    count_total = 0
    sin_nuevos_consecutivos = 0
    LIMITE_MENSAJES = 50000000000
    LIMITE_SIN_NUEVOS = 6000000

    # Paso 2: Usar get_chat_history hacia atrás desde el an21cla
    offset_id = id_mas_antiguo
    batch_size = 100
    
    while True:
        got_any = False
        try:
            async for message in client.get_chat_history(
                chat_id, offset_id=offset_id, limit=batch_size
            ):
                got_any = True
                offset_id = message.id  # avanza hacia IDs menores
                count_total += 1
                if count_total >= LIMITE_MENSAJES:
                    print(f"[STOP] Límite de {LIMITE_MENSAJES} mensajes alcanzado en {chat_id}.")
                    break

                if not message.video:
                    continue

                es_nuevo = await procesar_video_historico(message)
                if es_nuevo:
                    count_nuevos += 1
                    sin_nuevos_consecutivos = 0
                    print(f"[HISTORIA] ID {message.id}: {message.video.file_name or 'Sin Nombre'}")
                else:
                    sin_nuevos_consecutivos += 1
                    print(f"[EXISTE] ID {message.id} (Video ya conocido)")

                if sin_nuevos_consecutivos >= LIMITE_SIN_NUEVOS:
                    print(f"[STOP] {LIMITE_SIN_NUEVOS} sin nuevos seguidos en {chat_id}.")
                    break
        except FloodWait as fw:
            print(f"[FLOODWAIT] {fw.value}s en chat {chat_id}. Esperando...")
            await asyncio.sleep(fw.value)
            continue
        except Exception as e:
            print(f"[ERR] get_chat_history en {chat_id}: {e}")
            break

        if not got_any:
            break
        if count_total >= LIMITE_MENSAJES or sin_nuevos_consecutivos >= LIMITE_SIN_NUEVOS:
            break

    # Paso 3: Aviso final (sin tocar counters; se calculan en otra etapa)
    if count_nuevos > 0:
        print(f"--- Fin Historia {chat_id} ({chat_nombre}). +{count_nuevos} videos antiguos recuperados. ---")
    else:
        print(f"--- Fin Historia {chat_id} ({chat_nombre}). No se encontraron más videos antiguos. ---")

    # Marcar última revisión histórica
    async with aiosqlite.connect(DB_PATH) as db:
        await _ensure_last_hist_scan_column(db)
        await db.execute(
            "UPDATE chat_video_counts SET last_hist_scan = datetime('now') WHERE chat_id = ?",
            (chat_id,),
        )
        await db.commit()


async def main():
    client = get_client(clone_for_cli=True)
    await client.start()

    try:
        chats_a_revisar = await obtener_chats_con_historial()
        print(f"Chats con historial para expandir hacia atrás: {len(chats_a_revisar)}")

        for chat_id in chats_a_revisar:
            try:
                await escanear_historia_antigua(client, chat_id)
            except Exception as e:
                print(f"Error procesando historia de chat {chat_id}: {e}")
                # Espera breve para no saturar si hay errores masivos
                await asyncio.sleep(2) 

    finally:
        try:
            await client.stop()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())