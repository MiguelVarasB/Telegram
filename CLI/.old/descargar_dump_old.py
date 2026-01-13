import os
import time
import asyncio
import aiosqlite
import random
import json
import re
import logging 

from datetime import datetime
import sys
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from PIL import Image

# Importamos excepciones específicas
from pyrogram.errors import FloodWait, RPCError, Timeout 

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuración
from config import (
    DB_PATH, THUMB_FOLDER, API_ID, API_HASH, 
    BOT_POOL_TOKENS, FOLDER_SESSIONS,
    CACHE_DUMP_VIDEOS_CHANNEL_ID, JSON_FOLDER,
    BOT_BATCH_LIMIT, BOT_BATCH_COOLDOWN,
    BOT_WAIT_MIN, BOT_WAIT_MAX
)
from utils import save_image_as_webp, log_timing
from utils.database_helpers import ensure_column

# Logging
logging.basicConfig(level=logging.ERROR)
# Silenciar trazas de Pyrogram (solo criticidad alta)
pyro_root = logging.getLogger("pyrogram")
pyro_root.setLevel(logging.ERROR)
pyro_root.propagate = True

# --- VARIABLES GLOBALES DE TELEMETRÍA ---
sync_events = {}
estado_bots = {} 
floodwait_until = {}

# Estadísticas Globales
conteo_global = 0
errores_global = 0
inicio_time = time.time()

# Estadísticas Detalladas (NUEVO)
# Estructura: { bot_id: { 'dl': 0, 'floods': 0, 'boxes': 0, 'start_ts': 0.0 } }
bot_analytics = {} 

# Registro de Incidentes de Flood (NUEVO)
# Lista de dicts: [{ 'bot': 1, 'files_before': 10, 'time_before': 50s, 'wait': 300s }]
flood_incidents = []
tareas_totales = 0

FloodWait_forzado = 60*1
LIMITE_DB=999999

BOTS_BLOQUEADOS=[
       1, #Este nunca usarlo en este script
       2,
     #  4,
      # 6,
      9,
       # 10, 
      # 12,
      13,
      # 14,
      #15,
     # 16,
       17,
     # 20,
     #  21, 
     #22, 
     #  23,
    #  24,
     #25,
     26,
     # 27,
     #  28,
     #  29,
      30
]



# Semáforo Global (Ajustado a 3 por seguridad de auth.ExportAuthorization)
MAX_CONCURRENT_DOWNLOADS = 8
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


class LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.last_error_msg = ""
    def emit(self, record):
        self.last_error_msg = self.format(record)

# --- REPORTES Y METRICAS ---

def _floodwait_restante_s(bot_id: int) -> int:
    until_ts = floodwait_until.get(bot_id, 0) or 0
    return max(0, int(until_ts - time.time()))

def _registrar_incidente_flood(bot_id: int, descargas_session: int, batch_start_time: float, wait_s: int):
    """Guarda telemetría de flood/timeout para el resumen final."""
    tiempo_vivo = time.time() - batch_start_time
    incidente = {
        'bot': bot_id,
        'files_before': descargas_session,
        'time_before': tiempo_vivo,
        'wait': wait_s
    }
    flood_incidents.append(incidente)
    bot_analytics[bot_id]['floods'] += 1
    return tiempo_vivo

def generar_informe_calibracion():
    """Genera un reporte legible para ajustar parametros"""
    global conteo_global, inicio_time, bot_analytics, flood_incidents
    
    tiempo_total = time.time() - inicio_time
    if tiempo_total < 1: tiempo_total = 1
    
    lines = []
    lines.append("="*60)
    lines.append(f"📊 REPORTE DE CALIBRACIÓN Y TELEMETRÍA - {datetime.now()}")
    lines.append("="*60)
    lines.append(f"Duración Test: {int(tiempo_total)} seg ({round(tiempo_total/60, 2)} min)")
    lines.append(f"Descargas Totales: {conteo_global}")
    lines.append(f"Descargas por segundo: {round(conteo_global/tiempo_total, 2)}")
    lines.append(f"Config Actual: Batch={BOT_BATCH_LIMIT} | Concurrentes={MAX_CONCURRENT_DOWNLOADS}")
    lines.append("-" * 60)
    
    lines.append("\n🔎 1. RENDIMIENTO POR BOT INDIVIDUAL")
    lines.append(f"{'BOT ID':<8} | {'DLs':<6} | {'FLOODs':<8} | {'BOXES':<6} | {'PROM (arch/min)':<15}")
    lines.append("-" * 60)
    
    for b_id, stats in sorted(bot_analytics.items()):
        dls = stats.get('dl', 0)
        floods = stats.get('floods', 0)
        boxes = stats.get('boxes', 0)
        
        # Calcular tiempo activo real del bot
        my_start = stats.get('start_ts', inicio_time)
        active_time = time.time() - my_start
        if active_time < 1: active_time = 1
        
        speed_bpm = (dls / active_time) * 60 # Archivos por minuto
        
        lines.append(f"Bot {b_id:<4} | {dls:<6} | {floods:<8} | {boxes:<6} | {speed_bpm:.2f}")

    lines.append("-" * 60)
    lines.append("\n🔥 2. ANÁLISIS DE FLOOD (CAJA NEGRA)")
    
    if not flood_incidents:
        lines.append("✅ ¡Felicidades! No hubo incidentes de FloodWait.")
        lines.append("   -> Podrías intentar subir MAX_CONCURRENT_DOWNLOADS a 4.")
    else:
        lines.append(f"Se registraron {len(flood_incidents)} incidentes.")
        lines.append("El objetivo es ver CUÁNTO aguantaron antes de morir.")
        lines.append("")
        lines.append(f"{'BOT':<5} | {'ARCHIVOS ANTES DEL ERROR':<25} | {'TIEMPO VIVO (seg)':<20} | {'CASTIGO (seg)':<15}")
        
        sum_files = 0
        sum_time = 0
        
        for inc in flood_incidents:
            f_before = inc.get('files_before', 0)
            t_before = inc.get('time_before', 0)
            wait = inc.get('wait', 0)
            
            sum_files += f_before
            sum_time += t_before
            
            lines.append(f"#{inc['bot']:<4} | {f_before:<25} | {t_before:<20.1f} | {wait:<15}")
            
        avg_files = sum_files / len(flood_incidents)
        avg_time = sum_time / len(flood_incidents)
        
        lines.append("-" * 60)
        lines.append("💡 CONCLUSIÓN Y RECOMENDACIÓN:")
        lines.append(f"Promedio de archivos antes de Flood: {avg_files:.1f}")
        lines.append(f"Promedio de tiempo antes de Flood:   {avg_time:.1f} segundos")
        lines.append("")
        rec_limit = int(avg_files * 0.8) # Recomendamos el 80% del promedio
        if rec_limit < 1: rec_limit = 1
        
        lines.append(f"👉 RECOMENDACIÓN: Configura BOT_BATCH_LIMIT = {rec_limit}")
        lines.append(f"   (Esto hará que descansen antes de llegar al punto de quiebre promedio)")

    # Guardar a archivo
    try:
        os.makedirs(JSON_FOLDER, exist_ok=True)
        path_txt = os.path.join(JSON_FOLDER, "reporte_calibracion.txt")
        with open(path_txt, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        log_timing(f"\n📄 Reporte detallado guardado en: {path_txt}")
        # Imprimir resumen en consola
        log_timing("\n".join(lines))
    except Exception as e:
        log_timing(f"Error guardando reporte txt: {e}")

async def monitor_loops():
    while True:
        await asyncio.sleep(15)
        # Exportar JSON simple (existente)
        # ... (Tu funcion exportar_estado_bots_json existente)

# --- DB ---
async def check_database_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await ensure_column(db, "videos_telegram", "dump_fail", "INTEGER", "0")
        await ensure_column(db, "videos_telegram", "thumb_bytes", "INTEGER")

async def get_tareas_pendientes():
    log_timing("   [SQL] Consultando videos pendientes...")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, file_unique_id, dump_message_id, chat_id 
            FROM videos_telegram 
            WHERE has_thumb = 0 AND dump_message_id IS NOT NULL
              AND (dump_fail IS NULL OR dump_fail = 0)
            ORDER BY fecha_mensaje DESC LIMIT ?
        """, (LIMITE_DB,)) as c:
            rows = await c.fetchall()
            log_timing(f"   [SQL] {len(rows)} videos encontrados.")
            return rows

async def marcar_completado(vid_id, thumb_size=None):
    global conteo_global, tareas_totales
    async with aiosqlite.connect(DB_PATH) as db:
        if thumb_size is None:
            await db.execute("UPDATE videos_telegram SET has_thumb = 1 WHERE id = ?", (vid_id,))
        else:
            await db.execute(
                "UPDATE videos_telegram SET has_thumb = 1, thumb_bytes = ? WHERE id = ?",
                (thumb_size, vid_id),
            )
        await db.commit()
    conteo_global += 1
    if tareas_totales:
        if conteo_global % 100 == 0 or conteo_global == tareas_totales:
            log_timing(f"[Progreso] Descargados {conteo_global} de {tareas_totales}")

async def marcar_fallido(vid_id, razon):
    # Simplificado para no llenar logs
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE videos_telegram SET dump_fail = 1 WHERE id = ?", (vid_id,))

# --- WORKER BOT ---
async def worker_bot(queue, bot_token, bot_id):
    global bot_analytics, flood_incidents
    
    session_path = os.path.join(FOLDER_SESSIONS, f"bot_worker_{bot_id}")
    app = Client(session_path, api_id=API_ID, api_hash=API_HASH, bot_token=bot_token)
    
    pyro_logger = logging.getLogger("pyrogram")
    log_capture = LogCapture()
    
    # Inicializar Telemetría del Bot
    bot_analytics[bot_id] = {
        'dl': 0, 
        'floods': 0, 
        'boxes': 0, 
        'start_ts': time.time()
    }
    
    # Variables de sesión (Se resetean en cada "Box" o Flood)
    descargas_session = 0 
    batch_start_time = time.time()
    
    estado_bots[bot_id] = "UNKNOWN"
    floodwait_until[bot_id] = 0

    try:
        await app.start()
        estado_bots[bot_id] = "WORKING"
        log_timing(f"   [Bot {bot_id}] Iniciado.")

        # Bot 1 conserva su índice/sesión pero no procesa tareas
        if bot_id == 1:
            estado_bots[bot_id] = "SKIPPED"
            while True:
                tarea_skip = await queue.get()
                if tarea_skip is None:
                    queue.task_done()
                    break
                # Devolver la tarea al pool para otros bots
                queue.put_nowait(tarea_skip)
                queue.task_done()
                await asyncio.sleep(0.1)
            return

        while True:
            tarea = await queue.get()
            if tarea is None: break
            
            vid_id, unique_id, msg_id, chat_origin = tarea
            final_path = os.path.join(THUMB_FOLDER, str(chat_origin), f"{unique_id}.webp")

            # Check rapido si existe
            if os.path.exists(final_path) and os.path.getsize(final_path) > 200:
                try:
                    thumb_size = os.path.getsize(final_path)
                except Exception:
                    thumb_size = None
                await marcar_completado(vid_id, thumb_size)
                queue.task_done()
                continue

            try:
                await asyncio.sleep(random.uniform(BOT_WAIT_MIN, BOT_WAIT_MAX))

                # Obtener mensaje
                try:
                    msg = await app.get_messages(CACHE_DUMP_VIDEOS_CHANNEL_ID, msg_id)
                except Exception:
                    msg = None

                if not msg or msg.empty:
                    await marcar_fallido(vid_id, "Borrado")
                    continue

                # Identificar media
                media = msg.video or msg.document or msg.photo
                target_file_id = None
                if media:
                    if hasattr(media, "thumbs") and media.thumbs: target_file_id = media.thumbs[-1].file_id
                    elif msg.photo: target_file_id = media.file_id
                
                if not target_file_id:
                    await marcar_fallido(vid_id, "Sin Thumb")
                    continue

                # Descargar
                tmp_dir = os.path.join(THUMB_FOLDER, "_tmp")
                os.makedirs(tmp_dir, exist_ok=True)
                tmp_name = os.path.join(tmp_dir, f"{unique_id}_{bot_id}")

                exito = False
                log_capture.last_error_msg = ""
                pyro_logger.addHandler(log_capture)

                try:
                    async with download_semaphore:
                        down = await app.download_media(target_file_id, file_name=tmp_name)

                    # Verificar integridad
                    if not down or not os.path.exists(down) or os.path.getsize(down) == 0:
                        err_msg = log_capture.last_error_msg
                        log_timing(f"   [Bot {bot_id}] Detalle error previo al Flood forzado: {err_msg}")
                        wait_t = FloodWait_forzado
                        if "FLOOD" in err_msg.upper() or "420" in err_msg:
                            match = re.search(r"wait.*?(\d+)", err_msg, re.IGNORECASE)
                            wait_t = int(match.group(1)) if match else 1000
                            raise FloodWait(wait_t) # Lanzamos para captura de telemetría
                        elif "503" in err_msg or "Timeout" in err_msg:
                             raise Timeout()
                        raise FloodWait(wait_t)

                    try:
                        with Image.open(down) as img: img.verify()
                    except:
                        if os.path.exists(down): os.remove(down)
                        raise FloodWait(60) # Corrupto = Flood simulado

                    # ÉXITO
                    await asyncio.to_thread(save_image_as_webp, down, final_path)
                    try:
                        thumb_size = os.path.getsize(final_path)
                    except Exception:
                        thumb_size = None
                    await marcar_completado(vid_id, thumb_size)
                    log_timing(f"   [Bot {bot_id}] OK: {unique_id}")
                    
                    # Telemetría de éxito
                    bot_analytics[bot_id]['dl'] += 1
                    descargas_session += 1
                    exito = True
                    floodwait_until[bot_id] = 0
                    if os.path.exists(down): os.remove(down)

                except Timeout:
                    log_timing(f"   [Bot {bot_id}] ⏳ Timeout 503. Reintentando...")
                    _registrar_incidente_flood(bot_id, descargas_session, batch_start_time, 30)
                    if down and os.path.exists(down): os.remove(down)
                    await asyncio.sleep(30)
                    await queue.put(tarea)

                except RPCError as e_rpc:
                    if "503" in str(e_rpc) or "Timeout" in str(e_rpc):
                        _registrar_incidente_flood(bot_id, descargas_session, batch_start_time, 30)
                        await asyncio.sleep(30)
                        await queue.put(tarea)
                    else:
                        raise e_rpc

                except FloodWait as e_fw:
                    # Capturamos el evento para telemetría antes de relanzar
                    raise e_fw 

                except Exception as e_inner:
                    log_timing(f"   [Bot {bot_id}] Error Red: {e_inner}")
                    if down and os.path.exists(down): os.remove(down)
                    await asyncio.sleep(10)
                    await queue.put(tarea)

                finally:
                    pyro_logger.removeHandler(log_capture)

                # --- CONTROL DE BOXES ---
                if exito and descargas_session >= BOT_BATCH_LIMIT:
                    log_timing(f"   [Bot {bot_id}] 💤 Boxes ({BOT_BATCH_COOLDOWN}s)...")
                    bot_analytics[bot_id]['boxes'] += 1
                    await asyncio.sleep(BOT_BATCH_COOLDOWN)
                    
                    # Resetear contadores de sesión (empezamos ciclo nuevo limpio)
                    descargas_session = 0
                    batch_start_time = time.time()

            except FloodWait as e_sleep:
                wait_s = e_sleep.value
                
                _registrar_incidente_flood(bot_id, descargas_session, batch_start_time, wait_s)

                floodwait_until[bot_id] = time.time() + wait_s
                estado_bots[bot_id] = "FLOODWAIT"
                
                log_timing(f"   [Bot {bot_id}] 🛑 FLOOD {wait_s}s. (Hizo {descargas_session} en {time.time() - batch_start_time:.1f}s)")
                
                await asyncio.sleep(wait_s)
                
                # Reinicio post-trauma
                floodwait_until[bot_id] = 0
                estado_bots[bot_id] = "WORKING"
                descargas_session = 0 
                batch_start_time = time.time()
                
                await queue.put(tarea)

            except Exception as e_fatal:
                log_timing(f"   [Bot {bot_id}] ❌ Error Fatal: {e_fatal}")
                await marcar_fallido(vid_id, str(e_fatal))

            finally:
                queue.task_done()

    finally:
        try: await app.stop()
        except: pass

async def main(recycle_when_all_floodwait=False):
    global conteo_global, errores_global, inicio_time, tareas_totales
    conteo_global, errores_global = 0, 0
    inicio_time = time.time()
    
    os.makedirs(FOLDER_SESSIONS, exist_ok=True)
    await check_database_schema()
    
    tareas = await get_tareas_pendientes()
    tareas_iniciales = len(tareas)
    tareas_totales = tareas_iniciales

    if not tareas:
        return {"tareas_iniciales": 0, "descargas": 0, "errores": 0, "recycled": False}

    # Si recycle_when_all_floodwait, verificar si todos los bots están en floodwait
    if recycle_when_all_floodwait:
        now = time.time()
        all_in_flood = all(
            floodwait_until.get(i+1, 0) > now 
            for i in range(len(BOT_POOL_TOKENS))
        ) if floodwait_until else False
        if all_in_flood:
            return {"tareas_iniciales": tareas_iniciales, "descargas": 0, "errores": 0, "recycled": True}

    log_timing(f" Iniciando V8 (TELEMETRÍA + SEMÁFORO={MAX_CONCURRENT_DOWNLOADS})...+ {len(BOT_POOL_TOKENS)} Bots - {len(BOTS_BLOQUEADOS)} Bots bloqueados ")
    queue = asyncio.Queue()
    for t in tareas: queue.put_nowait(t)

    bot_tasks = []
    for i, token in enumerate(BOT_POOL_TOKENS):
        bot_id = i + 1
        if bot_id in BOTS_BLOQUEADOS:
            log_timing(f"   [Bot {bot_id}] Saltado (en BOTS_BLOQUEADOS)")
            continue
        bot_tasks.append(asyncio.create_task(worker_bot(queue, token, bot_id)))

    await queue.join()
    for _ in BOT_POOL_TOKENS: await queue.put(None)
    await asyncio.gather(*bot_tasks, return_exceptions=True)
    
    return {
        "tareas_iniciales": tareas_iniciales,
        "descargas": conteo_global,
        "errores": errores_global,
        "recycled": False
    }

if __name__ == "__main__":
    try: 
        result = asyncio.run(main())
        log_timing(result)
    except KeyboardInterrupt: pass
    finally:
        # Aquí se genera el reporte mágico
        generar_informe_calibracion()