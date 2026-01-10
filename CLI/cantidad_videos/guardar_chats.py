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
async def guardar_chats(limit: Optional[int] = None) -> None:
    """
    Recorre todos los diálogos del usuario (grupos, supergrupos y canales)
    y los guarda/actualiza en la base de datos principal.
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
    guardados = 0

    try:
        async for dialog in client.get_dialogs(limit=limit):
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

            try:
                await db_upsert_chat_from_ci(chat, last_message_date=last_message_date, activo=1)
                guardados += 1
                nombre = chat.title or getattr(chat, "first_name", "") or str(chat.id)
                log_timing(f"💾 Guardado: {nombre} (id={chat.id}) tipo={chat.type}")
            except FloodWait as e:
                log_timing(f"⏳ FloodWait de {e.value}s al guardar {chat.id}, esperando...")
                await asyncio.sleep(e.value)
                continue
            except Exception as e:
                log_timing(f"⚠️ Error guardando chat {chat.id}: {e}")
    finally:
        await client.stop()
        log_timing("🛑 Cliente de Telegram detenido")
        ## contar chat activos segun la base de datos
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM chats WHERE activo = 1") as cursor:
                row = await cursor.fetchone()
             
      

        log_timing(f"Resumen: vistos={total_vistos}, guardados/actualizados={guardados} | chat activos: {row[0]}")


if __name__ == "__main__":
    # Siempre recorre todos los diálogos (sin límite)
    asyncio.run(guardar_chats(limit=None))
