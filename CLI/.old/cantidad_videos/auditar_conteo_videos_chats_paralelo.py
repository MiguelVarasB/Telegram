import asyncio
import os
import sys
import datetime
import aiosqlite
from concurrent.futures import ThreadPoolExecutor

from pyrogram import enums
from pyrogram.errors import FloodWait

"""Versión PARALELA optimizada: aprovecha múltiples núcleos para contar videos en paralelo."""

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client
from database import (
    init_db,
    db_upsert_chat_basic,
    db_upsert_chat_video_count,
)
from config import DB_PATH
from utils import log_timing

UMBRAL_DIAS = -1
UMBRAL_MINUTOS = 180
MAX_WORKERS = 8  # Número de workers paralelos (ajusta según tu CPU)

async def procesar_chat(client, chat_id: int, name: str, chat_type: str, username: str, 
                        last_msg_str: str, scanned_at: str, videos_count_bd: int,
                        ahora: datetime.datetime, umbral) -> dict | None:
    """Procesa un chat individual y retorna el resultado."""
    titulo = name or "Sin Nombre"
    
    # Usar fecha de último mensaje desde la base
    last_date = None
    try:
        if last_msg_str:
            last_date = datetime.datetime.fromisoformat(last_msg_str)
    except Exception:
        pass

    # Si UMBRAL_DIAS == -1, saltar si ya se escaneó hace <UMBRAL_MINUTOS
    if UMBRAL_DIAS < 0:
        try:
            if scanned_at:
                scanned_dt = datetime.datetime.fromisoformat(scanned_at)
                if scanned_dt >= ahora - datetime.timedelta(minutes=UMBRAL_MINUTOS):
                    return {"saltado": True, "razon": "escaneo_reciente"}
        except Exception:
            pass
    else:
        # Saltar si hay conteo previo y última fecha es > UMBRAL_DIAS días atrás
        tiene_conteo = videos_count_bd is not None and videos_count_bd > 0
        if tiene_conteo and last_date and last_date < umbral:
            return {"saltado": True, "razon": "antiguedad"}

    try:
        fecha_log = last_date.isoformat() if last_date else "N/D"
        
        # Contar videos en este chat
        count = await client.search_messages_count(
            chat_id, 
            filter=enums.MessagesFilter.VIDEO
        )

        # Guardar en BD
        scanned_at_new = datetime.datetime.utcnow().isoformat()
        await db_upsert_chat_basic(
            chat_id=chat_id,
            name=titulo,
            chat_type=chat_type,
            username=username,
            raw_json=None,
            ultimo_escaneo=scanned_at_new,
        )
        await db_upsert_chat_video_count(chat_id, count, scanned_at_new)
        
        return {
            "saltado": False,
            "titulo": titulo[:35],
            "id": chat_id,
            "videos": count,
            "fecha": fecha_log,
        }
        
    except FloodWait as e:
        log_timing(f"⚠️ FloodWait {e.value}s en chat {chat_id}, esperando...")
        await asyncio.sleep(e.value)
        # Reintentar
        return await procesar_chat(client, chat_id, name, chat_type, username,
                                  last_msg_str, scanned_at, videos_count_bd, ahora, umbral)
    except Exception as err:
        log_timing(f"❌ Error en chat {chat_id}: {err}")
        return {"saltado": True, "razon": "error"}


async def procesar_batch_chats(client, chats_batch: list, ahora: datetime.datetime, umbral) -> list:
    """Procesa un batch de chats en paralelo."""
    tasks = []
    for chat_data in chats_batch:
        task = procesar_chat(client, *chat_data, ahora, umbral)
        tasks.append(task)
    
    # Ejecutar todos en paralelo
    resultados = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in resultados if r and not isinstance(r, Exception)]


async def contar_videos_en_todos_mis_chats_paralelo():
    await init_db()
    client = get_client(clone_for_cli=True)

    if not client.is_connected:
        await client.start()

    log_timing("\n" + "="*70)
    log_timing("🎬 AUDITORÍA DE VIDEOS POR CANAL (MODO PARALELO)")
    log_timing("="*70)
    log_timing(f"📡 Procesando con {MAX_WORKERS} workers paralelos\n")
    
    resultados = []
    procesados = 0
    saltados = 0
    saltados_por_antiguedad = 0
    
    try:
        ahora = datetime.datetime.utcnow()
        umbral = ahora - datetime.timedelta(days=UMBRAL_DIAS) if UMBRAL_DIAS >= 0 else None
        
        # Obtener todos los chats de la BD
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """
                SELECT
                    c.chat_id,
                    c.name,
                    c.type,
                    c.username,
                    COALESCE(c.last_message_date, 'N/A') AS last_message_date,
                    c.ultimo_escaneo,
                    COALESCE(cv.videos_count, 0) AS videos_count_bd
                FROM chats c
                LEFT JOIN chat_video_counts cv ON c.chat_id = cv.chat_id
                WHERE c.activo = 1
                  AND COALESCE(c.is_owner, 0) = 0
                ORDER BY c.last_message_date DESC
                """
            ) as cur:
                chats_rows = await cur.fetchall()

        total_chats = len(chats_rows)
        
        # Si no hay chats en BD, obtenerlos directamente de Telegram
        if total_chats == 0:
            log_timing("⚠️ No hay chats en BD. Obteniendo directamente de Telegram...")
            from database import db_upsert_chat_from_ci
            
            chats_rows = []
            async for dialog in client.get_dialogs():
                chat = dialog.chat
                if chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL):
                    # Guardar en BD para futuras ejecuciones
                    last_message_date = None
                    try:
                        # Intentar obtener fecha del último mensaje
                        top_msg = getattr(dialog, 'top_message', None)
                        if top_msg and hasattr(top_msg, 'date'):
                            last_message_date = top_msg.date.isoformat()
                    except Exception:
                        pass
                    
                    await db_upsert_chat_from_ci(chat, last_message_date=last_message_date, activo=1)
                    
                    # Añadir a la lista para procesar
                    chats_rows.append((
                        chat.id,
                        chat.title or str(chat.id),
                        str(chat.type),
                        getattr(chat, 'username', None),
                        last_message_date or 'N/A',
                        None,  # ultimo_escaneo
                        0      # videos_count_bd
                    ))
            
            total_chats = len(chats_rows)
            log_timing(f"✅ Obtenidos {total_chats} chats de Telegram")
        
        log_timing(f"📊 Total de chats a procesar: {total_chats}")
        
        # Procesar en batches para evitar sobrecarga
        BATCH_SIZE = MAX_WORKERS * 2
        for i in range(0, len(chats_rows), BATCH_SIZE):
            batch = chats_rows[i:i+BATCH_SIZE]
            log_timing(f"🔄 Procesando batch {i//BATCH_SIZE + 1}/{(total_chats + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch)} chats)...")
            
            resultados_batch = await procesar_batch_chats(client, batch, ahora, umbral)
            
            for res in resultados_batch:
                if res.get("saltado"):
                    saltados += 1
                    if res.get("razon") == "antiguedad":
                        saltados_por_antiguedad += 1
                else:
                    procesados += 1
                    if res.get("videos", 0) > 0:
                        resultados.append(res)
            
            # Pequeña pausa entre batches para no saturar
            await asyncio.sleep(0.5)

        # Mostrar reporte final
        log_timing("\n" + "📊 REPORTE FINAL (Ordenado por volumen)")
        log_timing("-" * 75)
        log_timing(f"{'CANAL / GRUPO':<35} | {'VIDEOS':<10} | {'ID'}")
        log_timing("-" * 75)
        
        resultados.sort(key=lambda x: x['videos'], reverse=True)
        
        for res in resultados:
            log_timing(f"{res['titulo']:<35} | {res['videos']:<10} | {res['id']}")

        log_timing("-" * 75)
        log_timing(f"✅ Total de canales con videos: {len(resultados)}")
        log_timing(f"📄 Procesados: {procesados} | Saltados: {saltados} (antigüedad>{UMBRAL_DIAS}d: {saltados_por_antiguedad})")

    except Exception as e:
        log_timing(f"❌ Error general: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(contar_videos_en_todos_mis_chats_paralelo())
