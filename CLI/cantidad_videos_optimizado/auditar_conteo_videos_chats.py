import asyncio
import os
import sys
import datetime
import aiosqlite

from pyrogram import enums
from pyrogram.errors import FloodWait

"""Resumen: Auditoría concurrente de videos por chat (Optimizado para Xeon/Multihilo)."""

# Ajuste de ruta para tus servicios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client
from database import (
    init_db,
    db_upsert_chat_basic,
    db_upsert_chat_video_count,
)
from config import DB_PATH
from utils import log_timing

# CONFIGURACIÓN
UMBRAL_DIAS = -1      # -1 para ignorar antigüedad y usar solo escaneo reciente
UMBRAL_MINUTOS = 180  # Si se escaneó hace menos de X minutos, se salta
CONCURRENCY = 5      # Número de chats simultáneos (Tu Xeon aguanta esto y más)

async def procesar_chat_individual(sem, client, row, ahora, fecha_limite):
    """
    Procesa un único chat manejando sus propias esperas y conexiones.
    Retorna el dict de resultados o None si fue saltado/error.
    """
    chat_id, name, chat_type, username, last_msg_str, scanned_at, videos_count_bd = row
    titulo = name or "Sin Nombre"
    
    # --- FILTRADO LÓGICO EN PYTHON (Más rápido y seguro que SQL complejo) ---
    
    # 1. Parsear fechas
    last_date = None
    if last_msg_str and last_msg_str != 'N/A':
        try:
            last_date = datetime.datetime.fromisoformat(last_msg_str)
        except: pass

    # 2. Lógica de Salto (Skip)
    # Saltar chats muy recientes con pocos videos
    if last_date and last_date >= ahora - datetime.timedelta(days=2) and videos_count_bd <= 0:
        log_timing(f"⏭️ Saltado ( por antiguedad): {titulo[:20]}...") 
        return None

    if UMBRAL_DIAS < 0:
        # Modo "Refresco": Saltar solo si se escaneó hace muy poco (ej. < 3 horas)
        if scanned_at:
            try:
                scanned_dt = datetime.datetime.fromisoformat(scanned_at)
                if scanned_dt >= ahora - datetime.timedelta(minutes=UMBRAL_MINUTOS):
                    log_timing(f"⏭️ Saltado (Reciente): {titulo[:20]}...") 
                    return None # Saltamos silenciosamente para no ensuciar log
            except: pass
    else:
        # Modo "Antigüedad": Saltar si el último mensaje es muy viejo Y ya tenemos un conteo
        tiene_conteo = videos_count_bd is not None and videos_count_bd > 0
        if tiene_conteo and last_date and fecha_limite and last_date < fecha_limite:
            return None

    # --- PROCESAMIENTO CONCURRENTE ---
    async with sem: # Limitamos a X tareas simultáneas
        try:
            # Bucle de reintentos para FloodWait
            while True:
                try:
                    count = await client.search_messages_count(
                        chat_id, 
                        filter=enums.MessagesFilter.VIDEO
                    )
                    break # Éxito, salimos del while
                except FloodWait as e:
                    log_timing(f"⚠️ FloodWait en {titulo[:15]}: Esperando {e.value}s... (Otros hilos siguen)")
                    await asyncio.sleep(e.value)
                    # Reintentamos automáticamente después de dormir
            
            # Guardado en BD (Cada tarea gestiona su escritura)
            new_scanned_at = datetime.datetime.utcnow().isoformat()
            
            # Actualizamos metadatos básicos
            await db_upsert_chat_basic(
                chat_id=chat_id,
                name=titulo,
                chat_type=chat_type,
                username=username,
                ultimo_escaneo=new_scanned_at,
                activo=1 # Confirmamos que está activo
            )
            # Actualizamos conteo
            await db_upsert_chat_video_count(chat_id, count, new_scanned_at)

            fecha_log = last_date.isoformat() if last_date else "N/D"
            if count > 0:
                log_timing(f"✅ Procesado: {titulo[:30]}... ({count} videos)")
                return {
                    "titulo": titulo[:35],
                    "id": chat_id,
                    "videos": count,
                    "fecha": fecha_log
                }
            else:
                log_timing(f"ℹ️ Vacío: {titulo[:30]}... (0 videos)")
                return None

        except Exception as e:
            log_timing(f"❌ Error en {chat_id}: {e}")
            return None

async def contar_videos_en_todos_mis_chats():
    await init_db()
    client = get_client(clone_for_cli=True)

    if not client.is_connected:
        await client.start()

    log_timing("\n" + "="*70)
    log_timing(f"🚀 AUDITORÍA PARALELA ({CONCURRENCY} HILOS) - MIS CHATS")
    log_timing("="*70)

    ahora = datetime.datetime.utcnow()
    fecha_limite = None
    if UMBRAL_DIAS >= 0:
        fecha_limite = ahora - datetime.timedelta(days=UMBRAL_DIAS)
        log_timing(f"📅 Filtrando chats inactivos desde: {fecha_limite.isoformat()}")
    else:
        log_timing(f"🔄 Modo Refresco: Re-escaneando todo lo no revisado en últimos {UMBRAL_MINUTOS} min.")

    # 1. OBTENCIÓN MASIVA DE CHATS (SQL SIMPLE)
    # Traemos TODOS los chats activos y que no son míos (canales/grupos externos)
    # Filtramos en Python para evitar bugs de fechas en SQLite
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT
                c.chat_id,
                c.name,
                c.type,
                c.username,
                COALESCE(c.last_message_date, 'N/A'),
                c.ultimo_escaneo,
                COALESCE(cv.videos_count, 0)
            FROM chats c
            LEFT JOIN chat_video_counts cv ON c.chat_id = cv.chat_id
            WHERE c.activo = 1
              AND COALESCE(c.is_owner, 0) = 0
            ORDER BY c.last_message_date DESC
            """
        ) as cur:
            chats_rows = await cur.fetchall()

    log_timing(f"📋 Total chats candidatos en BD: {len(chats_rows)}")
    log_timing("⚡ Iniciando trabajadores...")

    # 2. LANZAMIENTO DE TAREAS (ASYNCIO GATHER)
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = []
    
    for row in chats_rows:
        task = procesar_chat_individual(sem, client, row, ahora, fecha_limite)
        tasks.append(task)

    # Esperar a que terminen todos (con barra de progreso implícita por los logs)
    resultados_raw = await asyncio.gather(*tasks)
    
    # Filtrar Nones (saltados o errores)
    resultados = [r for r in resultados_raw if r is not None]