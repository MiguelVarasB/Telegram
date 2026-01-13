import asyncio
import os
import sys
import datetime
import aiosqlite

from pyrogram import enums
from pyrogram.errors import FloodWait

"""Resumen: Recorre todos tus diálogos y guarda en BD el conteo de videos por chat."""
"""
   el objetivo de este codigo es recorrer los chats activos de la base de datos, filtrando 
   por la fecha del ultimo mensaje y si no tiene ultimo_escaneo.
   y obtener desde la api de telegram el conteo de videos en cada chat.
    """
# Ajuste de ruta para tus servicios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client
from database import (
    init_db,
    db_upsert_chat_basic,
    db_upsert_chat_video_count,
)
from database.chats import db_get_chat_scan_meta, db_get_chat
from config import DB_PATH
from utils import log_timing
UMBRAL_DIAS = -1
UMBRAL_MINUTOS = 180
async def contar_videos_en_todos_mis_chats():
    await init_db()
    client = get_client(clone_for_cli=True)

    if not client.is_connected:
        await client.start()

    log_timing("\n" + "="*70)
    log_timing("🎥 AUDITORÍA DE VIDEOS POR CANAL (MIS CHATS)")
    log_timing("="*70)
    log_timing(f"📡 Analizando diálogos (umbral de actividad: {UMBRAL_DIAS} días). Por favor espera.\n")
    
    resultados = []
    procesados = 0
    saltados = 0
    saltados_por_antiguedad = 0
    
    try:
        ahora = datetime.datetime.utcnow()
        umbral = ahora - datetime.timedelta(days=UMBRAL_DIAS) if UMBRAL_DIAS >= 0 else None
        if umbral:
            log_timing(f"fecha minima: {umbral.isoformat()}.\n")
        else:
            log_timing("Modo refresco por escaneo reciente (<1h).")
        # 1. Obtenemos la lista de chats desde la base de datos (sin llamar a get_dialogs)
        fecha_min = (ahora - datetime.timedelta(days=UMBRAL_DIAS)).isoformat()
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
                  AND (
                        c.ultimo_escaneo IS NULL
                     OR c.last_message_date > datetime('now', ? || ' days')
                  )
                ORDER BY c.last_message_date DESC
                """,
                (fecha_min,),
            ) as cur:
                chats_rows = await cur.fetchall()

        for chat_id, name, chat_type, username, last_msg_str, scanned_at, videos_count_bd in chats_rows:
            titulo = name or "Sin Nombre"
            # 2. Filtramos solo Grupos, Supergrupos y Canales (ya filtrado en SQL)

            # Usar fecha de último mensaje desde la base (chats.last_message_date) si existe
            last_date = None
            try:
                if last_msg_str:
                    last_date = datetime.datetime.fromisoformat(last_msg_str)
            except Exception:
                last_date = None

            # Si UMBRAL_DIAS == -1, saltar si ya se escaneó hace <1h usando scanned_at
            if UMBRAL_DIAS < 0:
                try:
                    if scanned_at:
                        scanned_dt = datetime.datetime.fromisoformat(scanned_at)
                        if scanned_dt >= ahora - datetime.timedelta(minutes=UMBRAL_MINUTOS):
                            saltados += 1
                            log_timing(
                                f"⏭️ Saltado por escaneo reciente (<1h): {titulo[:30]}... "
                                f"último_scan={scanned_dt.isoformat()} id={chat_id}"
                            )
                            continue
                except Exception:
                    pass
            else:
                # Saltar si hay conteo previo y última fecha es > UMBRAL_DIAS días atrás
                # Usamos videos_count_bd que ya viene de la query principal
                tiene_conteo = videos_count_bd is not None and videos_count_bd > 0
                if tiene_conteo and last_date and last_date < umbral:
                    saltados += 1
                    saltados_por_antiguedad += 1
                    log_timing(
                        f"⏭️ Saltado por antigüedad (> {UMBRAL_DIAS}d): {titulo[:30]}... "
                        f"última={last_date.isoformat()} id={chat_id}"
                    )
                    continue

            try:
                fecha_log = last_date.isoformat() if last_date else "N/D"
                # 3. Contamos los videos en este chat específico
                count = await client.search_messages_count(
                    chat_id, 
                    filter=enums.MessagesFilter.VIDEO
                )

                # 3.1 Guardamos/actualizamos chat y conteo en BD
                scanned_at = datetime.datetime.utcnow().isoformat()
                await db_upsert_chat_basic(
                    chat_id=chat_id,
                    name=titulo,
                    chat_type=chat_type,
                    username=username,
                    raw_json=None,
                    ultimo_escaneo=scanned_at,
                )
                await db_upsert_chat_video_count(chat_id, count, scanned_at)
                
                procesados += 1
                if count > 0:
                    resultados.append({
                        "titulo": titulo[:35],
                        "id": chat_id,
                        "videos": count,
                        "fecha": fecha_log,
                    })
                    log_timing(f"✅ Procesado: {titulo[:30]}... ({count} videos) última={fecha_log}")
                else:
                    log_timing(f"ℹ️ Procesado: {titulo[:30]}... (0 videos) última={fecha_log}")
                    
            except FloodWait as e:
                # Si Telegram nos frena, esperamos lo que pida
                log_timing(f"⚠️ Esperando {e.value} segundos por límite de Flood...")
                await asyncio.sleep(e.value)
            except Exception as err:
                log_timing(f"❌ Error en chat {chat_id}: {err}")
                continue

        # 4. Mostrar reporte final ordenado por cantidad de videos
        log_timing("\n" + "📊 REPORTE FINAL (Ordenado por volumen)")
        log_timing("-" * 75)
        log_timing(f"{'CANAL / GRUPO':<35} | {'VIDEOS':<10} | {'ID'}")
        log_timing("-" * 75)
        
        # Ordenamos de mayor a menor cantidad de videos
        resultados.sort(key=lambda x: x['videos'], reverse=True)
        
        for res in resultados:
            log_timing(f"{res['titulo']:<35} | {res['videos']:<10} | {res['id']}")

        log_timing("-" * 75)
        log_timing(f"✅ Total de canales con videos: {len(resultados)}")
        log_timing(f"📄 Procesados: {procesados} | Saltados: {saltados} (antigüedad>{UMBRAL_DIAS}d con conteo previo: {saltados_por_antiguedad})")

    except Exception as e:
        log_timing(f"❌ Error general: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(contar_videos_en_todos_mis_chats())