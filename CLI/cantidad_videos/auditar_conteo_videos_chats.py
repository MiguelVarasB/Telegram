import asyncio
import os
import sys
import datetime
import aiosqlite

from pyrogram import enums
from pyrogram.errors import FloodWait

"""Resumen: Recorre todos tus diálogos y guarda en BD el conteo de videos por chat."""

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

UMBRAL_DIAS = -1
UMBRAL_MINUTOS = 180
async def contar_videos_en_todos_mis_chats():
    await init_db()
    client = get_client(clone_for_cli=True)

    if not client.is_connected:
        await client.start()

    print("\n" + "="*70)
    print("🎥 AUDITORÍA DE VIDEOS POR CANAL (MIS CHATS)")
    print("="*70)
    print(f"📡 Analizando diálogos (umbral de actividad: {UMBRAL_DIAS} días). Por favor espera.\n")
    
    resultados = []
    procesados = 0
    saltados = 0
    saltados_por_antiguedad = 0
    
    try:
        ahora = datetime.datetime.utcnow()
        umbral = ahora - datetime.timedelta(days=UMBRAL_DIAS) if UMBRAL_DIAS >= 0 else None
        if umbral:
            print(f"fecha minima: {umbral.isoformat()}.\n")
        else:
            print("Modo refresco por escaneo reciente (<1h).")
        # 1. Obtenemos la lista de chats desde la base de datos (sin llamar a get_dialogs)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                """
                SELECT c.chat_id, c.name, c.type, c.username, c.last_message_date, cv.scanned_at
                FROM chats c
                LEFT JOIN chat_video_counts cv ON cv.chat_id = c.chat_id
                WHERE c.type IN ('CHANNEL','GROUP','SUPERGROUP')
                AND c.activo = 1
                """
            ) as cur:
                chats_rows = await cur.fetchall()

        for chat_id, name, chat_type, username, last_msg_str, scanned_at in chats_rows:
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
                            print(
                                f"⏭️ Saltado por escaneo reciente (<1h): {titulo[:30]}... "
                                f"último_scan={scanned_dt.isoformat()} id={chat_id}"
                            )
                            continue
                except Exception:
                    pass
            else:
                # Saltar si hay conteo previo y última fecha es > UMBRAL_DIAS días atrás
                try:
                    meta = await db_get_chat_scan_meta(chat_id)
                    tiene_conteo = meta is not None and meta.get("videos_count") is not None
                    if tiene_conteo and last_date and last_date < umbral:
                        saltados += 1
                        saltados_por_antiguedad += 1
                        print(
                            f"⏭️ Saltado por antigüedad (> {UMBRAL_DIAS}d): {titulo[:30]}... "
                            f"última={last_date.isoformat()} id={chat_id}"
                        )
                        continue
                except Exception:
                    pass

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
                    print(f"✅ Procesado: {titulo[:30]}... ({count} videos) última={fecha_log}")
                else:
                    print(f"ℹ️ Procesado: {titulo[:30]}... (0 videos) última={fecha_log}")
                    
            except FloodWait as e:
                # Si Telegram nos frena, esperamos lo que pida
                print(f"⚠️ Esperando {e.value} segundos por límite de Flood...")
                await asyncio.sleep(e.value)
            except Exception as err:
                print(f"❌ Error en chat {chat_id}: {err}")
                continue

        # 4. Mostrar reporte final ordenado por cantidad de videos
        print("\n" + "📊 REPORTE FINAL (Ordenado por volumen)")
        print("-" * 75)
        print(f"{'CANAL / GRUPO':<35} | {'VIDEOS':<10} | {'ID'}")
        print("-" * 75)
        
        # Ordenamos de mayor a menor cantidad de videos
        resultados.sort(key=lambda x: x['videos'], reverse=True)
        
        for res in resultados:
            print(f"{res['titulo']:<35} | {res['videos']:<10} | {res['id']}")

        print("-" * 75)
        print(f"✅ Total de canales con videos: {len(resultados)}")
        print(f"📄 Procesados: {procesados} | Saltados: {saltados} (antigüedad>{UMBRAL_DIAS}d con conteo previo: {saltados_por_antiguedad})")

    except Exception as e:
        print(f"❌ Error general: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(contar_videos_en_todos_mis_chats())