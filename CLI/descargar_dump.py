import os
import time
import asyncio
import aiosqlite
import random
import logging 
import re
from datetime import datetime
import sys
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, Timeout, ServiceUnavailable 
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importamos configuración central
from config import (
    DB_PATH, THUMB_FOLDER, API_ID, API_HASH, 
    BOT_POOL_TOKENS, FOLDER_SESSIONS,
    CACHE_DUMP_VIDEOS_CHANNEL_ID, JSON_FOLDER,
    BOT_BATCH_LIMIT, BOT_BATCH_COOLDOWN,
    BOT_WAIT_MIN, BOT_WAIT_MAX
)
from utils import save_image_as_webp, log_timing

# --- CONFIGURACIÓN DE LOGGING (Restaurada LogCapture) ---
logging.basicConfig(level=logging.ERROR)
pyro_logger = logging.getLogger("pyrogram")
pyro_logger.setLevel(logging.ERROR)

class LogCapture(logging.Handler):
    """Clase para atrapar errores que Telegram solo manda al log"""
    def __init__(self):
        super().__init__()
        self.last_error_msg = ""
    def emit(self, record):
        self.last_error_msg = self.format(record)

log_capture = LogCapture()
pyro_logger.addHandler(log_capture)

# --- VARIABLES GLOBALES ---
exitos_total = 0
borrados_total = 0   
sin_thumb_total = 0  
procesados_total = 0
inicio_time = time.time()
tareas_totales = 0 
bot_analytics = {} 
flood_incidents = []

# Diccionario de control de sueño global
floodwait_until = {}

# --- CONFIGURACIÓN DE CONCURRENCIA ---
MAX_CAJEROS = 8        
TIMEOUT_OPERACION = int(os.getenv("DOWNLOAD_TIMEOUT_S", "20"))
LIMITE_DB = 999999  
BOTS_BLOQUEADOS = [1, 6,11] 

semaforo = asyncio.Semaphore(MAX_CAJEROS)
stats_mon = {"activos": 0}

# --- REPORTES ---

def generar_informe_calibracion():
    """Genera el reporte final estilo old con análisis de Flood"""
    global exitos_total, borrados_total, sin_thumb_total, procesados_total, inicio_time, bot_analytics, tareas_totales
    tiempo_total = time.time() - inicio_time
    if tiempo_total < 1: tiempo_total = 1
    
    lines = ["="*60, f"📊 REPORTE DE PROCESAMIENTO V9.4 - {datetime.now()}", "="*60]
    lines.append(f"Duración: {int(tiempo_total)}s | Tareas: {tareas_totales} | Procesados: {procesados_total}")
    lines.append(f"  ✅ Exitosos: {exitos_total} | 🖼️ f=2: {sin_thumb_total} | 🗑️ f=3: {borrados_total}")
    lines.append("-" * 60)
    
    lines.append(f"{'BOT ID':<8} | {'DLs':<6} | {'FLOODs':<8} | {'BOXES':<6} | {'PROM (a/m)':<10}")
    for b_id, s in sorted(bot_analytics.items()):
        active_t = time.time() - s['start_ts']
        prom = (s['dl'] / max(1, active_t)) * 60
        lines.append(f"Bot {b_id:<4} | {s['dl']:<6} | {s['floods']:<8} | {s['boxes']:<6} | {prom:.2f}")

    os.makedirs(JSON_FOLDER, exist_ok=True)
    with open(os.path.join(JSON_FOLDER, "reporte_calibracion.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n" + "\n".join(lines))

# --- LÓGICA ---

async def procesar_descarga(app, bot_id, tarea):
    """Lógica de descarga con verificación de integridad"""
    vid_id, unique_id, msg_id, chat_origin = tarea
    final_path = os.path.join(THUMB_FOLDER, str(chat_origin), f"{unique_id}.webp")

    # Reiniciamos captura de errores para esta tarea
    log_capture.last_error_msg = ""

    msg = await app.get_messages(CACHE_DUMP_VIDEOS_CHANNEL_ID, msg_id)
    if not msg or msg.empty:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE videos_telegram SET dump_fail = 3 WHERE id = ?", (vid_id,))
            await db.commit()
        return "ERR_MSG_EMPTY"

    media = msg.video or msg.document or msg.photo
    file_id = media.thumbs[-1].file_id if (media and hasattr(media, "thumbs") and media.thumbs) else (media.file_id if msg.photo else None)
    
    if not file_id:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE videos_telegram SET dump_fail = 2 WHERE id = ?", (vid_id,))
            await db.commit()
        return "ERR_NO_THUMB"

    tmp_path = os.path.join(THUMB_FOLDER, "_tmp", f"b{bot_id}_{unique_id}")
    path = await app.download_media(file_id, file_name=tmp_path)
    
    # Verificamos si hubo un error silencioso capturado por LogCapture
    err_log = log_capture.last_error_msg.upper()
    if "420" in err_log or "FLOOD" in err_log:
        match = re.search(r"WAIT OF (\d+)", err_log)
        wait_s = int(match.group(1)) if match else 600
        raise FloodWait(wait_s)

    # Verificación de imagen (Evita error PIL cannot identify)
    if path and os.path.exists(path) and os.path.getsize(path) > 100:
        try:
            with Image.open(path) as img: img.verify()
            await asyncio.to_thread(save_image_as_webp, path, final_path)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE videos_telegram SET has_thumb = 1, dump_fail = 0 WHERE id = ?", (vid_id,))
                await db.commit()
            if os.path.exists(path): os.remove(path)
            return "SUCCESS"
        except:
            if os.path.exists(path): os.remove(path)
            return "RETRY"
    
    if path and os.path.exists(path): os.remove(path)
    return "RETRY"

async def worker_bot(queue, bot_token, bot_id):
    """Bucle principal con memoria de Flood"""
    global exitos_total, borrados_total, sin_thumb_total, procesados_total, floodwait_until
    session_path = os.path.join(FOLDER_SESSIONS, f"bot_worker_{bot_id}")
    app = Client(session_path, api_id=API_ID, api_hash=API_HASH, bot_token=bot_token)
    
    bot_analytics[bot_id] = {'dl': 0, 'floods': 0, 'boxes': 0, 'start_ts': time.time()}
    dls_session = 0

    try:
        await app.start()
        log_timing(f"   [Bot {bot_id}] 🟢 Conectado.")

        while True:
            # MEMORIA DE FLOOD: ¿Debo seguir durmiendo?
            if bot_id in floodwait_until:
                restante = floodwait_until[bot_id] - time.time()
                if restante > 0:
                    await asyncio.sleep(min(restante, 15))
                    continue
                else:
                    del floodwait_until[bot_id]

            tarea = await queue.get()
            if tarea is None: break
            
            try:
                await asyncio.sleep(random.uniform(BOT_WAIT_MIN, BOT_WAIT_MAX))

                async with semaforo:
                    stats_mon["activos"] += 1
                    try:
                        res = await asyncio.wait_for(procesar_descarga(app, bot_id, tarea), timeout=TIMEOUT_OPERACION)
                        
                        if res == "SUCCESS":
                            procesados_total += 1
                            exitos_total += 1
                            bot_analytics[bot_id]['dl'] += 1
                            dls_session += 1
                            log_timing(f"   [Bot {bot_id}] ✅ OK: {tarea[1]}")
                        elif res == "ERR_MSG_EMPTY":
                            procesados_total += 1
                            borrados_total += 1
                        elif res == "ERR_NO_THUMB":
                            procesados_total += 1
                            sin_thumb_total += 1
                        else:
                            await queue.put(tarea)

                    except FloodWait as e:
                        floodwait_until[bot_id] = time.time() + e.value
                        bot_analytics[bot_id]['floods'] += 1
                        log_timing(f"   [Bot {bot_id}] 🛑 FLOOD {e.value}s. Se va a dormir.")
                        await queue.put(tarea)

                    except (asyncio.TimeoutError, ServiceUnavailable, Timeout):
                        procesados_total += 1
                        log_timing(f"   [Bot {bot_id}] ⚠️ Timeout Red. Se descarta esta tarea en esta sesión.")

                    finally:
                        stats_mon["activos"] -= 1

                if dls_session >= int(BOT_BATCH_LIMIT):
                    bot_analytics[bot_id]['boxes'] += 1
                    await asyncio.sleep(BOT_BATCH_COOLDOWN)
                    dls_session = 0

            except Exception as e:
                # Captura de errores de red que no son FloodWait nativos
                err_str = str(e).upper()
                if "420" in err_str or "FLOOD" in err_str:
                    wait_s = int(re.search(r'\d+', err_str).group()) if re.search(r'\d+', err_str) else 600
                    floodwait_until[bot_id] = time.time() + wait_s
                    log_timing(f"   [Bot {bot_id}] 🛑 Error 420 detectado. Durmiendo {wait_s}s.")
                else:
                    log_timing(f"   [Bot {bot_id}] ❌ Error: {str(e)[:50]}")
                await queue.put(tarea)
            finally:
                queue.task_done()
    finally:
        await app.stop()

async def main(recycle_when_all_floodwait: bool = False):
    global tareas_totales
    log_timing("🚀 Iniciando Motor V9.4 (Con LogCapture y Memoria de Flood)")
    log_timing(f"⏱️ TIMEOUT_OPERACION={TIMEOUT_OPERACION}s")
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"""
            SELECT id, file_unique_id, dump_message_id, chat_id 
            FROM videos_telegram 
            WHERE has_thumb = 0 AND dump_message_id IS NOT NULL
              AND (dump_fail IS NULL OR dump_fail = 0)
            ORDER BY fecha_mensaje DESC LIMIT {LIMITE_DB}
        """) as c:
            tareas = await c.fetchall()

    log_timing(f"📋 Tareas encontradas: {len(tareas)}")

    if recycle_when_all_floodwait and floodwait_until:
        now = time.time()
        all_in_flood = all(ts > now for ts in floodwait_until.values())
        if all_in_flood:
            return {"tareas_iniciales": 0, "descargas": 0, "errores": 0, "recycled": True}

    if not tareas:
        return {"tareas_iniciales": 0, "descargas": 0, "errores": 0, "recycled": False}
    tareas_totales = len(tareas)
    queue = asyncio.Queue()
    for t in tareas: queue.put_nowait(t)

    bot_tasks = [asyncio.create_task(worker_bot(queue, token, i+1)) 
                 for i, token in enumerate(BOT_POOL_TOKENS) if (i+1) not in BOTS_BLOQUEADOS]

    async def monitor():
        while not queue.empty():
            await asyncio.sleep(10)
            dormidos = sum(1 for v in floodwait_until.values() if v > time.time())
            log_timing(f"📊 MONITOR: {procesados_total}/{tareas_totales} | {stats_mon['activos']}/8 cajas | 🛑 {dormidos} bots dormidos")

    mon_task = asyncio.create_task(monitor())
    await queue.join()
    for _ in bot_tasks: await queue.put(None)
    await asyncio.gather(*bot_tasks)
    mon_task.cancel()
    generar_informe_calibracion()
    return {
        "tareas_iniciales": tareas_totales,
        "descargas": exitos_total,
        "errores": borrados_total + sin_thumb_total,
        "recycled": False
    }

if __name__ == "__main__":
    try:
        res = asyncio.run(main())
        log_timing(res)
    except KeyboardInterrupt:
        generar_informe_calibracion()