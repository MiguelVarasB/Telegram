import argparse
import asyncio
import os
import sqlite3
import sys
from pathlib import Path

import aiosqlite
from pyrogram import enums
from utils import log_timing

# Asegurar import desde la raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import DB_PATH  # noqa: E402
from services.telegram_client import get_client  # noqa: E402
from services.video_processor import procesar_mensajes_video_batch  # noqa: E402

BATCH_SIZE = 50 

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

async def obtener_stats_chat(chat_id: int) -> tuple[int, int, int, int, int]:
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
            if not row: return 0, 0, 0, 0, 0
            return tuple(int(x or 0) for x in row)

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
        ORDER BY (cvc.videos_count - COALESCE(cvc.duplicados, 0) - COALESCE(cvc.indexados, 0)) DESC
    """
    cursor.execute(query)
    chats = [row["chat_id"] for row in cursor.fetchall()]
    if only_chat_id is not None:
        chats = [cid for cid in chats if cid == only_chat_id]
    if max_chats:
        chats = chats[:max_chats]
    conn.close()
    return chats

async def escanear_chat_inteligente(
    client,
    chat_id: int,
    consecutivos_para_detener: int = 30,
    max_nuevos_por_chat: int | None = None,
):
    # Stats iniciales
    indexados, total_unicos, _, _, faltantes = await obtener_stats_chat(chat_id)
    log_timing(f"\n🚀 Iniciando: Chat {chat_id} | Faltan aprox: {faltantes}")

    # CARGA A RAM
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT message_id FROM video_messages WHERE chat_id = ?", (chat_id,)) as cur:
            ids_locales = {row[0] async for row in cur}
    
    log_timing(f"🧠 RAM: {len(ids_locales)} IDs cargados.")

    consecutivos_existentes = 0
    nuevos_indexados = 0
    batch_pendientes = []

    async def flush_batch():
        nonlocal nuevos_indexados
        if not batch_pendientes: return False
        resultados = await procesar_mensajes_video_batch(batch_pendientes, origen="sincronizador")
        for i, res in enumerate(resultados):
            m = batch_pendientes[i]
            nombre = m.video.file_name or "sin_nombre"
            status = "✨ NUEVO" if res.get("mensaje_nuevo") else "🔗 GAP"
            log_timing(f"  {status}: {nombre[:30]} | msg_id={m.id}")
            if res.get("mensaje_nuevo"): nuevos_indexados += 1
        
        batch_pendientes.clear()
        return (max_nuevos_por_chat and nuevos_indexados >= max_nuevos_por_chat)

    try:
        # Usamos get_chat_history para el "papiro" secuencial
        async for m in client.get_chat_history(chat_id):
            if not m.video: continue
            
            nombre_vid = m.video.file_name or "sin_nombre"
            
            if m.id in ids_locales:
                # Si hay algo pendiente, lo guardamos antes de contar consecutivos
                if batch_pendientes:
                    if await flush_batch(): break

                consecutivos_existentes += 1
                # LOG "PAPIRO": Uno por cada video existente
                log_timing(f"  ℹ️  Ya existe: {nombre_vid[:30]} | msg_id={m.id} ({consecutivos_existentes}/{consecutivos_para_detener})")
                
                if consecutivos_existentes >= consecutivos_para_detener:
                    log_timing(f"  🛑 Límite alcanzado: {consecutivos_para_detener} seguidos.")
                    break
            else:
                consecutivos_existentes = 0
                batch_pendientes.append(m)
                log_timing(f"  🔍 Descubierto: {m.id} - Añadiendo al lote ({len(batch_pendientes)}/{BATCH_SIZE})")
                if len(batch_pendientes) >= BATCH_SIZE:
                    if await flush_batch(): break
        
        await flush_batch()

    except Exception as e:
        log_timing(f"❌ Error: {e}")

async def sync_con_stop(max_chats, only_chat_id, consecutivos, max_nuevos):
    client = get_client(clone_for_cli=True)
    await client.start()
    try:
        chats = obtener_chats_para_escanear(max_chats, only_chat_id)
        for cid in chats:
            await escanear_chat_inteligente(client, cid, consecutivos, max_nuevos)
    finally:
        await client.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-chats", type=int, default=None)
    parser.add_argument("--only-chat-id", type=int, default=None)
    parser.add_argument("--consecutivos", type=int, default=30)
    parser.add_argument("--max-nuevos-chat", type=int, default=None)
    args = parser.parse_args()
    
    asyncio.run(sync_con_stop(args.max_chats, args.only_chat_id, args.consecutivos, args.max_nuevos_chat))