import asyncio
import os
import re
import json
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

# === CONFIG DESDE config.py ===
from config import API_ID, API_HASH, BOT_TOKEN, BOT_POOL_TOKENS, FOLDER_SESSIONS, JSON_FOLDER

# Configuración
TIEMPO_DESCARGA = 3
CARPETA_SALIDA = "downloads"

# === SISTEMA DE CONTEO ===
cola_descargas = asyncio.Queue() # Aquí se guardan los trabajos pendientes
total_detectados = 0             # Total histórico de esta sesión
total_procesados = 0             # Total ya descargados

os.makedirs(FOLDER_SESSIONS, exist_ok=True)

# Usamos el primer bot del pool como monitor
monitor_session = os.path.join(FOLDER_SESSIONS, "monitor_bot_queue")
app = Client(monitor_session, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_POOL_TOKENS[0])

def _flood_wait_seconds(e: Exception) -> int:
    value = getattr(e, "value", None)
    if value is not None:
        try:
            return int(value)
        except Exception:
            pass

    m = re.search(r"wait of (\d+) seconds", str(e), flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 0

    return 0

def _format_seconds(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{mm:02d}:{ss:02d}"

def _serialize_message(msg) -> dict:
    # Intentamos obtener el dict raw (TLObject) si está disponible
    try:
        raw = getattr(msg, "_", None)
        if raw and hasattr(raw, "to_dict"):
            return raw.to_dict()
    except Exception:
        pass
    # Fallback: usamos __dict__ y dejamos que json convierta lo no serializable a str
    try:
        return msg.__dict__
    except Exception:
        return {"fallback_str": str(msg)}

async def _save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

async def _sleep_with_timer(seconds: float, label: str) -> None:
    remaining = int(round(seconds))
    if remaining <= 0:
        return

    while remaining > 0:
        print(f"\r   >> ⏳ {label}: {_format_seconds(remaining)} ", end="", flush=True)
        await asyncio.sleep(1)
        remaining -= 1
    print("\r", end="", flush=True)

async def trabajador_de_descargas():
    """ 
    Este es el 'empleado' que trabaja en el fondo descargando la fila.
    """
    global total_procesados
    
    print("👷 TRABAJADOR INICIADO: Esperando videos en la fila...")
    
    while True:
        # 1. Esperamos a que haya algo en la fila
        tarea = await cola_descargas.get()
        
        thumb_file_id, unique_id, chat_nombre, msg_dict = tarea
        
        # Actualizamos conteo visual
        pendientes = cola_descargas.qsize()
        print(f"\n🚜 PROCESANDO: {total_procesados + 1} de {total_detectados} (En espera: {pendientes})")
        print(f"   >> 📂 Origen: {chat_nombre}")

        # 2. Intentamos descargar (Lógica segura)
        ruta = os.path.join(CARPETA_SALIDA, f"{unique_id}.jpg")
        ruta_json = os.path.join(JSON_FOLDER, f"{unique_id}.json")
        try:
            await _save_json(ruta_json, msg_dict)
        except Exception as e:
            print(f"   >> ⚠️ No se pudo guardar JSON: {e}")
        
        # Sólo descargamos miniatura si tenemos thumb_file_id (i.e., es video con thumb)
        if thumb_file_id:
            if os.path.exists(ruta):
                 print(f"   >> ⏭️ Ya existe. Saltando.")
            else:
                await descargar_con_reintentos(app, thumb_file_id, ruta)
        else:
            print("   >> (Sin descarga) No es video con thumbnail; solo se guardó JSON.")
        
        # 3. Marcamos tarea como lista y sumamos procesado
        total_procesados += 1
        cola_descargas.task_done()

async def descargar_con_reintentos(client, file_id, path) -> bool:
    while True:
        try:
            await client.download_media(message=file_id, file_name=path)
            print(f"   >> ✅ Descarga completada.")

            # Pausa de seguridad
            await _sleep_with_timer(TIEMPO_DESCARGA, "Pausa de seguridad")
            return True

        except FloodWait as e:
            wait_s = _flood_wait_seconds(e)
            print(f"   >> 🛑 FLOODWAIT: Pausando {wait_s} segundos...")
            await _sleep_with_timer(wait_s + 2, "FloodWait")
            continue

        except Exception as e:
            if e.__class__.__name__ == "FloodWait" or "FLOOD_WAIT" in str(e):
                wait_s = _flood_wait_seconds(e)
                print(f"   >> 🛑 FLOODWAIT: Pausando {wait_s} segundos...")
                await _sleep_with_timer(wait_s + 2, "FloodWait")
                continue

            print(f"   >> ❌ Error: {e}")
            return False

# === EL OJO QUE TODO LO VE (Detector) ===
@app.on_message()
async def detector_rapido(client, message):
    global total_detectados
    
    thumb_file_id = None
    unique_id = f"{message.chat.id}_{message.id}"
    chat = message.chat.title or "Privado"
    msg_dict = _serialize_message(message)

    if message.video and message.video.thumbs:
        unique_id = message.video.file_unique_id or unique_id
        thumb = message.video.thumbs[-1]
        thumb_file_id = thumb.file_id

    # 1. Aumentamos el contador TOTAL inmediatamente
    total_detectados += 1
    
    # 2. Metemos el trabajo a la fila (NO descargamos aquí)
    item = (thumb_file_id, unique_id, chat, msg_dict)
    cola_descargas.put_nowait(item)
    
    # 3. Aviso rápido en consola
    pendientes = cola_descargas.qsize()
    print(f"👀 ¡DETECTADO! Total acumulados: {total_detectados} | En cola para bajar: {pendientes}")

async def main():
    # Creamos carpeta
    if not os.path.exists(CARPETA_SALIDA):
        os.makedirs(CARPETA_SALIDA)

    # Iniciamos el cliente
    await app.start()
    
    # Arrancamos al "trabajador" en segundo plano
    asyncio.create_task(trabajador_de_descargas())
    
    print("🤖 BOT LISTO. Envíame muchos videos de golpe...")
    
    # Mantenemos el bot corriendo (Idle)
    from pyrogram import idle
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())