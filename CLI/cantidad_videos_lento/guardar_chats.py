import asyncio
import os
import sys
import json
from pathlib import Path
from typing import Optional
import datetime

import aiosqlite
from pyrogram import enums
from pyrogram.errors import RPCError

# Permitir imports del proyecto (raíz /Telegram)
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.telegram_client import get_client
from services.unigram import obtener_fechas_y_ids
from database.chats import db_bulk_upsert_chats
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
    chats_to_save: list[tuple] = []
    BULK_SIZE = 200

    dialogs_iter = client.get_dialogs(limit=limit)

    try:
        while True:
            try:
                dialog = await dialogs_iter.__anext__()
            except StopAsyncIteration:
                break
            except RPCError as e:
                # Algunos canales/supergrupos devuelven CHANNEL_PRIVATE al pedir mensajes fijados.
                if "CHANNEL_PRIVATE" in str(e):
                    log_timing(f"⚠️ Canal privado omitido: {e}")
                    continue
                raise

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

            # Preparar datos para inserción masiva (sin raw_json pesado si no es necesario)
            name = chat.title or getattr(chat, "first_name", None) or "Sin Nombre"
            chat_type = str(chat.type).replace("ChatType.", "") if getattr(chat, "type", None) else None
            photo_id = chat.photo.small_file_id if getattr(chat, "photo", None) else None
            username = getattr(chat, "username", None)
            is_owner = getattr(chat, "is_creator", False) or getattr(chat, "is_self", False)
            is_public = bool(username)
            has_protected_content = getattr(chat, "has_protected_content", False)
            # Evitar serialización costosa: solo si se requiere raw_json más adelante
            raw_json = None

            chats_to_save.append(
                (
                    chat.id,
                    name,
                    chat_type,
                    photo_id,
                    username,
                    raw_json,
                    last_message_date,
                    None,  # ultimo_escaneo
                    datetime.datetime.utcnow().isoformat(),
                    1 if is_owner else 0,
                    1 if is_public else 0,
                    1 if has_protected_content else 0,
                    1,  # activo
                )
            )

            guardados += 1
            nombre_log = chat.title or getattr(chat, "first_name", "") or str(chat.id)
            log_timing(f"💾 Guardado: {nombre_log} (id={chat.id}) tipo={chat.type}")

            if len(chats_to_save) >= BULK_SIZE:
                log_timing(f"💾 Guardando bloque de {len(chats_to_save)} chats en base de datos...")
                await db_bulk_upsert_chats(chats_to_save)
                log_timing(f"✅ Bloque guardado ({len(chats_to_save)})")
                chats_to_save.clear()

        log_timing(f"Recolectados {len(chats_to_save)} chats pendientes. Guardando en base de datos (bulk final)...")
        if chats_to_save:
            await db_bulk_upsert_chats(chats_to_save)
            log_timing(f"💾 {len(chats_to_save)} chats guardados/actualizados en bloque final.")
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
