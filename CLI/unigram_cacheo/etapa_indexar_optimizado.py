"""
ETAPA INDEXAR - Estrategia Inversa (Ultra-Rápida)
==================================================
Optimización para Windows y Hardware de Alto Rendimiento (32 Cores).

ESTRATEGIA:
En lugar de enviar 400MB de mensajes a cada worker (lo que satura la RAM/Bus en Windows),
enviamos la lista de archivos buscados (20KB) a todos los workers y repartimos
los mensajes en bloques pequeños.
"""

import os
import struct
import re
import datetime
import time
from typing import List, Dict, Tuple, Set
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import sqlite3

# Importaciones locales
from .common import (
    CARPETAS,
    DB_UNIGRAM,
    preparar_base_local,
    obtener_duracion_video,
    cargar_mensajes_unigram,
)
from .config_optimizacion import (
    MAX_WORKERS, 
    DB_COMMIT_SIZE, 
    ENABLE_DETAILED_LOGGING
)

# Configuración de logs
import sys
# Ajusta esta ruta si es necesario o usa ruta relativa si utils está en path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils import log_timing
except ImportError:
    def log_timing(s): print(f"[{datetime.datetime.now().time()}] {s}")

# --- ESTRUCTURAS DE DATOS PARA WORKERS ---

def worker_scan_messages(messages_chunk: List[Tuple], target_huellas: Dict[bytes, str]) -> List[Dict]:
    """
    Worker optimizado: Recibe un fragmento de mensajes y busca CUALQUIER huella objetivo en ellos.
    Retorna las coincidencias encontradas.
    """
    matches = []
    
    # Pre-cachear longitudes para evitar llamadas en el bucle
    target_len = 8 # int64 son 8 bytes
    
    for m_id, d_id, data_blob in messages_chunk:
        if not data_blob or len(data_blob) < target_len:
            continue
            
        # Búsqueda optimizada: Verificar si alguna huella está en el blob
        # Python 'in' operator es muy rápido (implementado en C)
        for huella, nombre_archivo in target_huellas.items():
            if huella in data_blob:
                matches.append({
                    "archivo": nombre_archivo,
                    "canal_id": d_id,
                    "msg_id_global": m_id // 1048576
                })
                # No hacemos break porque un mensaje podría teóricamente contener referencias a múltiples archivos
                # o el mismo archivo estar referenciado múltiples veces (aunque raro)
                
    return matches

def _escanear_carpetas_rapido(archivos_ya_indexados: Set[str]) -> Tuple[List[Dict], List[Dict]]:
    """Identifica archivos nuevos y separa los que tienen ID de los que no."""
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    archivos_con_id = []
    archivos_sin_id = []
    
    # Escaneo paralelo de IO
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for tipo, ruta in CARPETAS.items():
            if os.path.exists(ruta):
                futures.append(executor.submit(_listar_dir, ruta, tipo, extensiones, archivos_ya_indexados))
        
        for future in as_completed(futures):
            con_id, sin_id = future.result()
            archivos_con_id.extend(con_id)
            archivos_sin_id.extend(sin_id)
            
    # Ordenar por fecha (más recientes primero)
    archivos_con_id.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return archivos_con_id, archivos_sin_id

def _listar_dir(ruta, tipo, extensiones, ya_indexados):
    con_id = []
    sin_id = []
    try:
        for f in os.listdir(ruta):
            if f in ya_indexados:
                continue
            if f.lower().endswith(extensiones) or f.isdigit():
                full_path = os.path.join(ruta, f)
                try:
                    info = {
                        "nombre": f, "tipo": tipo, "ruta": full_path,
                        "fecha_creacion": os.path.getctime(full_path),
                        "tamano": os.path.getsize(full_path)
                    }
                    if re.search(r"(\d{15,20})", f):
                        con_id.append(info)
                    else:
                        sin_id.append(info)
                except OSError: continue
    except OSError: pass
    return con_id, sin_id

def procesar_estrategia_inversa():
    """Motor principal de procesamiento."""
    conn_local, existentes = preparar_base_local()
    
    log_timing("📂 Escaneando sistema de archivos...")
    archivos_con_id, archivos_sin_id = _escanear_carpetas_rapido(existentes)
    
    if not archivos_con_id and not archivos_sin_id:
        log_timing("☕ No hay archivos nuevos.")
        conn_local.close()
        return

    log_timing(f"🎯 Nuevos: {len(archivos_con_id)} con ID (buscables), {len(archivos_sin_id)} sin ID.")

    cur_local = conn_local.cursor()
    fecha_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pendientes_commit = 0
    
    # 1. Guardar archivos SIN ID inmediatamente (no se pueden buscar en DB)
    if archivos_sin_id:
        log_timing(f"💾 Guardando {len(archivos_sin_id)} archivos sin ID...")
        for item in archivos_sin_id:
            dur = obtener_duracion_video(item["ruta"]) if item["tipo"] == "video" else None
            cur_local.execute(
                "INSERT OR IGNORE INTO cacheo (archivo, tipo, fecha_escaneo, encontrado, tamano_bytes, duracion_segundos) VALUES (?,?,?,0,?,?)",
                (item["nombre"], item["tipo"], fecha_now, item["tamano"], dur)
            )
        conn_local.commit()

    if not archivos_con_id:
        conn_local.close()
        return

    # 2. Preparar búsqueda inversa
    log_timing("🔄 Cargando y particionando mensajes...")
    _, todos_los_mensajes = cargar_mensajes_unigram()
    
    # Filtrar solo mensajes con blob válido
    msgs_validos = [m for m in todos_los_mensajes if m[2] is not None]
    total_msgs = len(msgs_validos)
    
    # Crear diccionario de búsqueda {huella_bytes: nombre_archivo}
    target_huellas = {}
    file_info_map = {} # Para acceso rápido a metadatos al guardar
    
    for item in archivos_con_id:
        match = re.search(r"(\d{15,20})", item["nombre"])
        if match:
            uid = int(match.group(1))
            huella = struct.pack("<q", uid)
            target_huellas[huella] = item["nombre"]
            file_info_map[item["nombre"]] = item

    # Calcular chunks de mensajes
    # Dividimos los mensajes entre los workers.
    # Windows start method 'spawn' es lento copiando datos, pero aquí copiamos
    # solo 1/32 de los datos a cada worker, lo cual es mucho más rápido.
    chunk_size = max(1, total_msgs // MAX_WORKERS)
    chunks = [msgs_validos[i:i + chunk_size] for i in range(0, total_msgs, chunk_size)]
    
    log_timing(f"🚀 Lanzando {len(chunks)} workers para escanear {total_msgs} mensajes contra {len(target_huellas)} archivos...")
    
    found_map = {} # nombre_archivo -> {datos_match}
    
    start_time = time.time()
    
    # EJECUCIÓN PARALELA
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviamos: chunk de mensajes (grande) + target_huellas (pequeño)
        futures = [executor.submit(worker_scan_messages, chunk, target_huellas) for chunk in chunks]
        
        for i, future in enumerate(as_completed(futures)):
            try:
                matches = future.result()
                for m in matches:
                    # Guardamos el primer match encontrado para cada archivo
                    if m["archivo"] not in found_map:
                        found_map[m["archivo"]] = m
                        if ENABLE_DETAILED_LOGGING:
                             log_timing(f"  ✅ MATCH: {m['archivo']} -> MSG {m['msg_id_global']}")
            except Exception as e:
                log_timing(f"❌ Error en worker: {e}")
                
            if (i+1) % 5 == 0:
                log_timing(f"📊 Progreso escaneo: {((i+1)/len(chunks))*100:.1f}%")

    scan_time = time.time() - start_time
    log_timing(f"🏁 Escaneo finalizado en {scan_time:.1f}s")
    
    # 3. Guardar resultados
    log_timing("💾 Guardando resultados en DB...")
    nuevos = 0
    encontrados = 0
    
    for item in archivos_con_id:
        nombre = item["nombre"]
        match = found_map.get(nombre)
        
        dur = obtener_duracion_video(item["ruta"]) if item["tipo"] == "video" else None
        
        if match:
            encontrados += 1
            cur_local.execute(
                """INSERT OR IGNORE INTO cacheo 
                   (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor)
                   VALUES (?, ?, ?, 1, ?, ?, ?, ?, 0)""",
                (nombre, item["tipo"], fecha_now, match["canal_id"], match["msg_id_global"], item["tamano"], dur)
            )
        else:
            # Archivo con ID pero no encontrado en los mensajes
            cur_local.execute(
                """INSERT OR IGNORE INTO cacheo 
                   (archivo, tipo, fecha_escaneo, encontrado, tamano_bytes, duracion_segundos, en_servidor)
                   VALUES (?, ?, ?, 0, ?, ?, 0)""",
                (nombre, item["tipo"], fecha_now, item["tamano"], dur)
            )
            
        nuevos += 1
        pendientes_commit += 1
        if pendientes_commit >= DB_COMMIT_SIZE:
            conn_local.commit()
            pendientes_commit = 0
            
    conn_local.commit()
    conn_local.close()
    log_timing(f"✨ Proceso terminado. Procesados: {nuevos}. Encontrados en DB: {encontrados}.")

def run_etapa_indexar_optimizado():
    procesar_estrategia_inversa()

if __name__ == "__main__":
    run_etapa_indexar_optimizado()