"""
Gestión de contadores cacheados en segundo plano (Worker).
Se encarga de mantener la tabla chat_video_counts actualizada sin bloquear la UI.
Incluye lógica de recálculo masivo seguro.
"""
import asyncio
import aiosqlite
import time
from config import DB_PATH
from utils import log_timing

# Semáforo para actualizaciones individuales (1 a la vez para no saturar)
_counter_semaphore = asyncio.Semaphore(1)

# Lock global para evitar que se solapen múltiples recálculos masivos
_global_recalc_lock = asyncio.Lock()

async def update_chat_stats_background(chat_id: int):
    """
    Función pública: Dispara la actualización de UN chat en segundo plano.
    Usado por: Listener de Telegram (cuando llega un video nuevo).
    """
    asyncio.create_task(_worker_update_stats(chat_id))

async def recalculate_all_stats_background():
    """
    Función pública: Dispara el recálculo MASIVO de TODOS los chats.
    Usado por: Home (al cargar la página principal).
    Es "Fire & Forget": retorna inmediatamente y deja el trabajo corriendo en background.
    Si ya hay un cálculo masivo corriendo, se salta esta llamada.
    """
    if _global_recalc_lock.locked():
        # Ya hay un cálculo masivo en curso, no es necesario encolar otro.
        return

    asyncio.create_task(_worker_recalculate_all())

async def _worker_update_stats(chat_id: int):
    """Lógica interna: Calcula estadísticas de un solo chat."""
    async with _counter_semaphore:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT 
                        COUNT(*),
                        SUM(CASE WHEN has_thumb = 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN es_vertical = 1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN duracion >= 3600 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN dump_fail = 1 AND dump_message_id IS NULL THEN 1 ELSE 0 END)
                    FROM videos_telegram
                    WHERE chat_id = ?
                """, (chat_id,)) as cursor:
                    row = await cursor.fetchone()
                
                if not row: return

                total, no_thumb, vertical, long_vid, blocked = row
                
                # Upsert (Insertar o Actualizar)
                await db.execute("""
                    INSERT INTO chat_video_counts 
                    (chat_id, videos_count, sin_thumb, vertical, duration_1h, blocked, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        videos_count = excluded.videos_count,
                        sin_thumb = excluded.sin_thumb,
                        vertical = excluded.vertical,
                        duration_1h = excluded.duration_1h,
                        blocked = excluded.blocked,
                        last_updated = CURRENT_TIMESTAMP
                """, (chat_id, total or 0, no_thumb or 0, vertical or 0, long_vid or 0, blocked or 0))
                
                await db.commit()

        except Exception as e:
            print(f"⚠️ Error actualizando stats chat {chat_id}: {e}")

async def _worker_recalculate_all():
    """Lógica interna: Recálculo masivo optimizado (GROUP BY)."""
    log_timing("🚀 Iniciando recálculo masivo de estadísticas...")
    async with _global_recalc_lock:
        try:
            # start = time.time()
            async with aiosqlite.connect(DB_PATH) as db:
                # 1. Calcular todo en memoria (Una sola lectura masiva es muy rápida)
                async with db.execute("""
                    SELECT 
                        chat_id,
                        COUNT(*) as total,
                        SUM(CASE WHEN has_thumb = 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN es_vertical = 1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN duracion >= 3600 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN dump_fail = 1 AND dump_message_id IS NULL THEN 1 ELSE 0 END)
                    FROM videos_telegram
                    GROUP BY chat_id
                """) as cursor:
                    rows = await cursor.fetchall()

                if not rows: return

                # 2. Preparar datos para inserción en lote
                # La query espera: (videos_count, sin_thumb, vertical, duration_1h, blocked, CHAT_ID)
                batch_data = []
                for r in rows:
                    # r = (chat_id, total, sin_thumb, vertical, duration_1h, blocked)
                    batch_data.append((
                        r[1] or 0, # videos_count
                        r[2] or 0, # sin_thumb
                        r[3] or 0, # vertical
                        r[4] or 0, # duration_1h
                        r[5] or 0, # blocked
                        r[0]       # chat_id
                    ))

                # 3. Escritura masiva
                await db.executemany("""
                    INSERT INTO chat_video_counts 
                    (videos_count, sin_thumb, vertical, duration_1h, blocked, chat_id, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        videos_count = excluded.videos_count,
                        sin_thumb = excluded.sin_thumb,
                        vertical = excluded.vertical,
                        duration_1h = excluded.duration_1h,
                        blocked = excluded.blocked,
                        last_updated = CURRENT_TIMESTAMP
                """, batch_data)
                
                await db.commit()
                log_timing(f"✅ Recálculo masivo completado: {len(batch_data)} chats actualizados")

        except Exception as e:
            log_timing(f"❌ Error en recálculo masivo: {e}")