import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

import aiosqlite
from pyrogram import enums
from pyrogram.errors import FloodWait, RPCError

# Permitir imports del proyecto (raíz /Telegram)
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.telegram_client import get_client
from services.unigram import obtener_fechas_y_ids
from database import db_upsert_chat_from_ci
from config import DB_PATH
from utils import log_timing
"""
   el objetivo de este codigo, es obtener todos los chats (grupos, supergrupos y canales)
    que tengo activos  y saber la fecha del  ultimo mensaje (usando puente con unigram)
    """

async def _procesar_batch(chats_batch: list) -> None:
    """Procesa un batch de chats en paralelo para máxima velocidad."""
    tasks = []
    for chat, last_message_date in chats_batch:
        task = db_upsert_chat_from_ci(chat, last_message_date=last_message_date, activo=1)
        tasks.append(task)
    
    # Ejecutar todas las inserciones en paralelo
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        log_timing(f"⚠️ Error en batch: {e}")
    
    # Log resumido cada batch
    if len(chats_batch) > 0:
        log_timing(f"💾 Batch procesado: {len(chats_batch)} chats")
async def guardar_chats(limit: Optional[int] = None) -> None:
    """
    Recorre todos los diálogos del usuario (grupos, supergrupos y canales)
    y los guarda/actualiza en la base de datos principal.
    Optimizado: procesa en batch para máxima velocidad.
    """
    # Cachear fechas/ids desde Unigram para usarlas como fuente confiable
    unigram_map = obtener_fechas_y_ids()

    # Resetear last_message_date a NULL para rehacer los valores con el escaneo actual
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE chats SET last_message_date = NULL, activo = 0")
        await db.commit()

    client = get_client(clone_for_cli=True)
    await client.start()
    log_timing("✅ Sesión Pyrogram iniciada (usuario).")

    total_vistos = 0
    chats_batch = []  # Acumular chats para procesamiento en batch
    BATCH_SIZE = 50  # Procesar cada 50 chats

    try:
        for dialog in client.get_dialogs(limit=limit):
            chat = dialog.chat

            # Solo interesan grupos, supergrupos y canales
            if chat.type not in (
                enums.ChatType.GROUP,
                enums.ChatType.SUPERGROUP,
                enums.ChatType.CHANNEL,
            ):
                continue

            total_vistos += 1
            last_message_date = None
            try:
                if dialog.message and dialog.message.date:
                    last_message_date = dialog.message.date.isoformat()
                elif getattr(dialog, "date", None):
                    last_message_date = dialog.date.isoformat()
            except Exception:
                pass

            try:
                data_uni = unigram_map.get(int(chat.id)) if isinstance(unigram_map, dict) else None
                fecha_uni = data_uni.get("fecha") if isinstance(data_uni, dict) else None
                if fecha_uni and fecha_uni != "N/A":
                    last_message_date = fecha_uni
            except Exception:
                pass

            # Loggear cada chat guardado para ver el detalle en consola
            log_timing(
                f"💾 Guardado: {chat.title or getattr(chat, 'first_name', 'Sin Nombre')} "
                f"(id={chat.id}) tipo={chat.type}"
            )

            # Preparar datos para inserción masiva (sin raw_json pesado si no es necesario)
            name = chat.title or getattr(chat, "first_name", None) or "Sin Nombre"
            username = getattr(chat, "username", None)
            raw_json = None  # evitar guardar JSON pesado si no se usa

            # Acumular en batch en lugar de guardar uno por uno
            chats_batch.append((chat, last_message_date))
            
            # Procesar batch cuando alcanza el tamaño
            if len(chats_batch) >= BATCH_SIZE:
                await _procesar_batch(chats_batch)
                chats_batch.clear()
        
        # Procesar chats restantes
        if chats_batch:
            await _procesar_batch(chats_batch)
    finally:
        await client.stop()
        log_timing("🛑 Cliente de Telegram detenido")
        ## contar chat activos segun la base de datos
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM chats WHERE activo = 1") as cursor:
                row = await cursor.fetchone()

        log_timing(f"Resumen: vistos={total_vistos}, guardados/actualizados en batch | chat activos: {row[0]}")


if __name__ == "__main__":
    # Siempre recorre todos los diálogos (sin límite)
    asyncio.run(guardar_chats(limit=None))
