import argparse
import asyncio
import os
import sqlite3
import sys
import uuid

import aiosqlite
from pyrogram import enums

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DB_PATH  # noqa: E402
from services.telegram_client import get_client  # noqa: E402
from database.videos import (  # noqa: E402
    db_add_video_file_id,
    db_upsert_video,
    db_upsert_video_message,
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


CANTIDAD_IDS = 5


# ---------------------------------------------------------
# 1. FUNCIÓN PARA OBTENER CHATS VÁLIDOS (videos_count > 0)
# ---------------------------------------------------------
def obtener_chats_para_escanear():
    conn = get_db()
    cursor = conn.cursor()
    # "si videos_count es 0 saltalo" -> Lo filtramos directo en el SQL
    cursor.execute("""
        SELECT chat_id 
        FROM chat_video_counts 
        WHERE videos_count > 0
    """)
    chats = [row['chat_id'] for row in cursor.fetchall()]
    conn.close()
    return chats


# ---------------------------------------------------------
# 2. FUNCIÓN PARA OBTENER LOS ÚLTIMOS 5 IDs
# ---------------------------------------------------------
def obtener_ultimos_ids(chat_id):
    conn = get_db()
    cursor = conn.cursor()
    # "obtenten los ultimos 5 message_id de video_messages"
    # Ordenamos DESC para tener los más recientes
    cursor.execute("""
        SELECT message_id 
        FROM video_messages 
        WHERE chat_id = ? 
        ORDER BY message_id DESC 
        LIMIT ?
    """, (chat_id, CANTIDAD_IDS))
    
    # Convertimos a un SET (conjunto) para búsqueda ultra rápida
    ids = {row['message_id'] for row in cursor.fetchall()}
    conn.close()
    return ids


# ---------------------------------------------------------
# 3. LÓGICA DE PROCESAMIENTO (INSERT/UPSERT)
# ---------------------------------------------------------
async def procesar_video_en_bd(message):
    """
    Intenta guardar el video usando helpers async de database.videos.
    Retorna True si era un video nuevo (por unique_id), False si ya existía.
    """
    video = message.video
    unique_id = video.file_unique_id

    async def _existe_video(file_unique_id: str) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM videos_telegram WHERE file_unique_id = ? LIMIT 1",
                (file_unique_id,),
            ) as cur:
                row = await cur.fetchone()
                return row is not None

    es_nuevo = not await _existe_video(unique_id)

    # Datos mínimos para upsert de video
    video_data = {
        "file_unique_id": unique_id,
        "chat_id": message.chat.id,
        "message_id": message.id,
        "file_id": video.file_id,
        "nombre": video.file_name,
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
    await db_upsert_video(video_data)
    await db_add_video_file_id(unique_id, video.file_id, unique_id, origen="sincronizador")

    # Datos de mensaje extendido
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
        }
        if from_user
        else {},
        "media": "video",
        "views": message.views or 0,
        "forwards": message.forwards or 0,
        "outgoing": message.outgoing,
        "reply_to_message_id": getattr(message, "reply_to_message_id", None),
        "forward_from_chat": {
            "id": getattr(forward_from_chat, "id", None),
            "title": getattr(forward_from_chat, "title", None),
        }
        if forward_from_chat
        else {},
        "forward_from_message_id": getattr(message, "forward_from_message_id", None),
        "forward_date": (
            message.forward_date.isoformat() if getattr(message, "forward_date", None) else None
        ),
        "caption": message.caption,
    }
    await db_upsert_video_message(message_data)

    return es_nuevo


# ---------------------------------------------------------
# 4. BUCLE PRINCIPAL DE ESCANEO
# ---------------------------------------------------------
async def escanear_chat_inteligente(client, chat_id: int):
    # Paso 1: Obtener los objetivos
    ids_objetivo = obtener_ultimos_ids(chat_id)
    
    # Si no hay historial previo, asumimos que debemos escanear todo (o manejamos como nuevo)
    if not ids_objetivo:
        print(f"Chat {chat_id}: No hay historial previo. Escaneando desde cero...")
        min_id_seguridad = 0
    else:
        min_id_seguridad = min(ids_objetivo) # El ID más bajo de los 5
        print(f"Chat {chat_id}: Buscando coincidencia con bloque de 5 IDs: {ids_objetivo}")

    coincidencias_encontradas = 0
    total_objetivos = len(ids_objetivo)

    # Paso 2: Recorrer Telegram (Search Messages + Filter Video)
    async for message in client.search_messages(chat_id, query="", filter=enums.MessagesFilter.VIDEO):
        
        # --- VERIFICACIÓN DE LOS 5 IDs ---
        
        if message.id in ids_objetivo:
            # "si está, usamos un contador" (y lo quitamos de la lista de buscados)
            coincidencias_encontradas += 1
            ids_objetivo.remove(message.id)
            print(f"[MATCH] Encontrado ID conocido: {message.id}. Faltan {len(ids_objetivo)}.")

            # "siga hasta que coincidan los 5 message_id"
            if len(ids_objetivo) == 0:
                print(f"--- ¡SINCRONIZACIÓN COMPLETA! Se encontraron los 5 IDs clave. Deteniendo. ---")
                break
        
        else:
            # "si no, agrega el video..."
            # Nota de seguridad: Solo agregamos si el ID es MAYOR que el mínimo que buscamos.
            # Si es MENOR, significa que el mensaje objetivo fue borrado de Telegram y nos pasamos de largo.
            
            if total_objetivos > 0 and message.id < min_id_seguridad:
                print(f"[ALERTA] ID {message.id} es menor que el objetivo más bajo ({min_id_seguridad}).")
                print("         Posiblemente algunos mensajes objetivo fueron borrados en Telegram.")
                print("         Deteniendo para evitar re-escanear todo el historial.")
                break
            
            # Procesamos el nuevo video
            es_nuevo = await procesar_video_en_bd(message)

            tag = "[NUEVO]" if es_nuevo else "[GAP FILL]"
            print(f"{tag} ID {message.id}: {message.video.file_name}")


async def main():
    client = get_client(clone_for_cli=True)
    await client.start()

    try:
        # 1. "que recorra ese listado" (de chats con videos)
        chats_a_revisar = obtener_chats_para_escanear()
        print(f"Se encontraron {len(chats_a_revisar)} chats activos para revisar.")

        for chat_id in chats_a_revisar:
            try:
                await escanear_chat_inteligente(client, chat_id)
            except Exception as e:
                print(f"Error procesando chat {chat_id}: {e}")
    finally:
        try:
            await client.stop()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza chats revisando los últimos 5 videos conocidos."
    )
    return parser.parse_args()


if __name__ == "__main__":
    _ = parse_args()
    asyncio.run(main())