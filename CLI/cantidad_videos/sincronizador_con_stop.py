import argparse
import asyncio
import os
import sqlite3
import sys
from pathlib import Path

import aiosqlite
from pyrogram import enums

"""Resumen: Sincroniza videos recientes por chat buscando coincidencia con los últimos IDs conocidos y detiene al encontrarlos."""
"""
   OBTEJAcá se deben obtener los canales/grupos (chats)  que tengan videos pendientes de indexar, y debe ser filtrado por los que estan activos y los que no son mios.
   Al realizar el loop por los videos, debe ir comprobando los mensajes en la base de datos (video_messages). si existen   X mensajes correlativos,  debe parar el escaneo del canal, si no existe el mensaje, se debe insertar el video y el mensaje en sus correspondientes tablas (hay funciones principales que se encargan de ello)
   
   si lleva contado x-1 pero aparece uno nuevo, debe resetear el contador
   CONSIDERAR QUE UN CANAL PUEDE TENER mas de 200K videos  y tengo mas de 800 canales, asi que debe ser eficiente
"""
# Asegurar import desde la raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


class FatalSyncError(RuntimeError):
    """Error fatal que debe detener todo el script."""


CANTIDAD_IDS = 10


# ---------------------------------------------------------
# Helper: obtener métricas del chat
# ---------------------------------------------------------
async def obtener_stats_chat(chat_id: int) -> tuple[int, int, int, int, int]:
    """
    Devuelve (indexados, total_unicos, total_videos, duplicados, faltantes)
    usando chat_video_counts. Si no existe registro, retorna ceros.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT 
                COALESCE(indexados, 0),
                COALESCE(videos_count, 0) - COALESCE(duplicados, 0) AS total_unicos,
                COALESCE(videos_count, 0),
                COALESCE(duplicados, 0),
                (COALESCE(videos_count, 0) - COALESCE(duplicados, 0) - COALESCE(indexados, 0)) AS faltantes
            FROM chat_video_counts
            WHERE chat_id = ?
            """,
            (chat_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return 0, 0, 0, 0, 0
            return tuple(int(x or 0) for x in row)


# ---------------------------------------------------------
# 1. FUNCIÓN PARA OBTENER CHATS VÁLIDOS (videos_count > 0)
# ---------------------------------------------------------
def obtener_chats_para_escanear(max_chats: int | None = None, only_chat_id: int | None = None):
    conn = get_db()
    cursor = conn.cursor()
    query = """
        SELECT cvc.chat_id
        FROM chat_video_counts cvc
        JOIN chats c ON cvc.chat_id = c.chat_id
        WHERE cvc.videos_count > 0
          AND c.activo = 1
          AND COALESCE(c.is_owner, 0) = 0
          AND (cvc.videos_count - COALESCE(cvc.duplicados, 0) - COALESCE(cvc.indexados, 0)) > 0
        ORDER BY (cvc.videos_count - COALESCE(cvc.duplicados, 0) - COALESCE(cvc.indexados, 0)) DESC
    """
    params = []
    if max_chats:
        query += " LIMIT ?"
        params.append(max_chats)
    cursor.execute(query, params)
    chats = [row["chat_id"] for row in cursor.fetchall()]
    if only_chat_id is not None:
        chats = [cid for cid in chats if cid == only_chat_id]
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

    async def _existe_mensaje(chat_id: int, message_id: int) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM video_messages WHERE chat_id = ? AND message_id = ? LIMIT 1",
                (chat_id, message_id),
            ) as cur:
                row = await cur.fetchone()
                return row is not None

    es_nuevo = not await _existe_mensaje(message.chat.id, message.id)

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
async def escanear_chat_inteligente(client, chat_id: int, consecutivos_para_detener: int = 30):
    indexados, total_unicos, total_videos, duplicados, faltantes = await obtener_stats_chat(chat_id)
    
    print(
        f"\n📺 Canal {chat_id}: {indexados} indexados / "
        f"{total_unicos} únicos ({total_videos} totales, {duplicados} dupes). "
        f"Faltan {faltantes}. Sincronizando hasta encontrar {consecutivos_para_detener} consecutivos existentes."
    )

    consecutivos_existentes = 0
    nuevos_indexados = 0
    gaps_rellenados = 0
    mensajes_recibidos = 0

    async def _existe_mensaje(chat_id: int, message_id: int) -> bool:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT 1 FROM video_messages WHERE chat_id = ? AND message_id = ? LIMIT 1",
                (chat_id, message_id),
            ) as cur:
                row = await cur.fetchone()
                return row is not None

    # Recorrer Telegram buscando solo videos (más eficiente que get_chat_history)
    async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=10000):
        mensajes_recibidos += 1

        # Proteger mensajes sin video (borrados o sin media)
        if not m.video:
            continue
        
        fn = m.video.file_name or "sin_nombre"
        
        # Verificar si el mensaje ya existe en video_messages
        ya_existe = await _existe_mensaje(chat_id, m.id)
        
        if ya_existe:
            # Incrementar contador de consecutivos
            consecutivos_existentes += 1
            print(
                f"  ℹ️  Ya existe: {fn[:40]} | msg_id={m.id} | chat_id={chat_id} "
                f"(consecutivos: {consecutivos_existentes}/{consecutivos_para_detener})"
            )
            
            # Si alcanzamos el umbral, detener
            if consecutivos_existentes >= consecutivos_para_detener:
                print(f"\n✅ Alcanzado umbral de {consecutivos_para_detener} consecutivos. Deteniendo escaneo.")
                break
        else:
            # Resetear contador cuando aparece uno nuevo
            if consecutivos_existentes > 0:
                print(f"  🔄 Contador reseteado (era {consecutivos_existentes})")
                consecutivos_existentes = 0
            
            # Procesar el nuevo video
            es_nuevo = await procesar_video_en_bd(m)
            
            if es_nuevo:
                nuevos_indexados += 1
                print(f"  ✨ Nuevo: {fn[:40]} | msg_id={m.id} | chat_id={chat_id} | {m.video.mime_type}")
            else:
                gaps_rellenados += 1
                print(f"  🔗 Gap rellenado: {fn[:40]} | msg_id={m.id} | chat_id={chat_id} | {m.video.mime_type}")
    
    print(
        f"\n📊 Resumen chat {chat_id}: {nuevos_indexados} nuevos, {gaps_rellenados} gaps rellenados. "
        f"Recibidos {mensajes_recibidos} mensajes con video (esperados {total_unicos} únicos)."
    )


async def sync_con_stop(max_chats: int | None = None, only_chat_id: int | None = None, consecutivos_para_detener: int = 30):
    client = get_client(clone_for_cli=True)
    await client.start()

    try:
        # 1. "que recorra ese listado" (de chats con videos)
        chats_a_revisar = obtener_chats_para_escanear(max_chats=max_chats, only_chat_id=only_chat_id)
        print(f"Se encontraron {len(chats_a_revisar)} chats activos para revisar.")

        for chat_id in chats_a_revisar:
            try:
                await escanear_chat_inteligente(client, chat_id, consecutivos_para_detener)
            except FatalSyncError as e:
                print(str(e))
                raise
            except Exception as e:
                print(f"Error procesando chat {chat_id}: {e}")
    finally:
        try:
            await client.stop()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza chats contando mensajes consecutivos existentes hasta alcanzar umbral."
    )
    parser.add_argument(
        "--max-chats",
        type=int,
        default=None,
        help="Límite de chats a procesar (ordenados por faltantes).",
    )
    parser.add_argument(
        "--only-chat-id",
        type=int,
        default=None,
        help="Si se indica, procesa solo ese chat.",
    )
    parser.add_argument(
        "--consecutivos",
        type=int,
        default=30,
        help="Número de mensajes consecutivos existentes para detener (default: 30).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(sync_con_stop(
        max_chats=args.max_chats,
        only_chat_id=args.only_chat_id,
        consecutivos_para_detener=args.consecutivos
    ))