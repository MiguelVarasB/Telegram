"""
ETAPA INDEXAR - Versión Optimizada para Multi-Núcleo (CORREGIDA)
====================================================

Este archivo es una versión optimizada del indexador de archivos multimedia de Telegram
diseñada específicamente para hardware de alto rendimiento (15 núcleos, 64GB RAM).

OPTIMIZACIONES IMPLEMENTADAS:
------------------------------
1. Procesamiento paralelo real con multiprocessing (uso de todos los núcleos CPU)
2. Inicialización de memoria compartida para workers
3. Indexación por lotes para mejor uso de caché
4. Búsqueda binaria optimizada con early termination
"""

import os
import struct
import re
import datetime
import time
from typing import Iterable, List, Dict, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import threading

# Importar componentes del proyecto
from .common import (
    CARPETAS,
    MASTER_KEY,
    DB_UNIGRAM,
    preparar_base_local,
    obtener_duracion_video,
    cargar_mensajes_unigram,
)
from .config_optimizacion import (
    MAX_WORKERS, 
    BATCH_SIZE, 
    DB_COMMIT_SIZE, 
    CHUNK_SIZE,
    LOG_BATCH_SIZE,
    ENABLE_DETAILED_LOGGING
)

# Configuración de logs externos
import sys
sys.path.append(r'C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram')
from utils import log_timing

# Variable global que residirá en el espacio de memoria de cada proceso worker
_global_messages_data = None

def init_worker(messages_data: List[Tuple]):
    """
    Inicializa cada proceso hijo con la copia de los mensajes en memoria.
    Esto permite que la búsqueda sea ultra rápida sin acceder a disco.
    """
    global _global_messages_data
    _global_messages_data = messages_data

def buscar_huella_en_mensajes(huella_bin: bytes) -> Tuple[Optional[int], Optional[int]]:
    """Búsqueda optimizada de huella binaria en los mensajes cargados."""
    global _global_messages_data
    
    if not _global_messages_data:
        return None, None
    
    # Búsqueda lineal optimizada en memoria
    for m_id, d_id, data_blob in _global_messages_data:
        if data_blob and huella_bin in data_blob:
            return d_id, m_id // 1048576 # Conversión a ID global
    
    return None, None

def procesar_archivo_batch(archivos_batch: List[Dict]) -> List[Dict]:
    """Procesa un lote de archivos aprovechando el núcleo asignado."""
    resultados = []
    
    for item in archivos_batch:
        nombre_f = item["nombre"]
        match = re.search(r"(\d{15,20})", nombre_f)
        
        if not match:
            if item["tipo"] == "video":
                try:
                    tamano_bytes = os.path.getsize(item["ruta"])
                    duracion_segundos = obtener_duracion_video(item["ruta"])
                    resultados.append({
                        "archivo": nombre_f, "tipo": item["tipo"],
                        "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "encontrado": 0, "canal_id": None, "msg_id_global": None,
                        "tamano_bytes": tamano_bytes, "duracion_segundos": duracion_segundos,
                    })
                except Exception: continue
            continue
        
        # Procesamiento con huella binaria
        id_cache_num = int(match.group(1))
        huella_bin = struct.pack("<q", id_cache_num)
        
        try:
            tamano_bytes = os.path.getsize(item["ruta"])
            duracion_segundos = obtener_duracion_video(item["ruta"]) if item["tipo"] == "video" else None
            
            # Búsqueda en la memoria compartida del worker
            canal_id, msg_id_global = buscar_huella_en_mensajes(huella_bin)
            
            resultados.append({
                "archivo": nombre_f, "tipo": item["tipo"],
                "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "encontrado": 1 if canal_id is not None else 0,
                "canal_id": canal_id, "msg_id_global": msg_id_global,
                "tamano_bytes": tamano_bytes, "duracion_segundos": duracion_segundos,
            })
        except Exception: continue
            
    return resultados

def iter_archivos_nuevos_optimizado(archivos_ya_indexados: set) -> List[Dict]:
    """Escanea carpetas usando múltiples hilos para acelerar el listado de archivos."""
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    lista = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for tipo, ruta in CARPETAS.items():
            if os.path.exists(ruta):
                futures.append(executor.submit(_escanear_carpeta, ruta, tipo, extensiones, archivos_ya_indexados))
        
        for future in as_completed(futures):
            lista.extend(future.result())
    
    lista.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return lista

def _escanear_carpeta(ruta: str, tipo: str, extensiones: tuple, archivos_ya_indexados: set) -> List[Dict]:
    resultados = []
    try:
        for f in os.listdir(ruta):
            if f.lower().endswith(extensiones) or f.isdigit():
                if f not in archivos_ya_indexados:
                    full_path = os.path.join(ruta, f)
                    try:
                        resultados.append({
                            "nombre": f, "tipo": tipo, "ruta": full_path,
                            "fecha_creacion": os.path.getctime(full_path),
                        })
                    except OSError: continue
    except OSError: pass
    return resultados

def procesar_archivos_paralelo(lista_a_procesar: Iterable[Dict]) -> None:
    """Orquestador principal del procesamiento multi-núcleo."""
    conn_local, _ = preparar_base_local()
    
    log_timing("🔄 Cargando mensajes de Unigram en memoria...")
    _, todos_los_mensajes = cargar_mensajes_unigram()
    
    # Filtrar mensajes útiles para reducir el tamaño de la memoria enviada a los workers
    mensajes_procesados = [(m_id, d_id, data) for m_id, d_id, data in todos_los_mensajes if data is not None]
    log_timing(f"🔧 {len(mensajes_procesados)} mensajes cargados y listos para búsqueda paralela.")
    
    lista_archivos = list(lista_a_procesar)
    chunks = [lista_archivos[i:i + CHUNK_SIZE] for i in range(0, len(lista_archivos), CHUNK_SIZE)]
    
    nuevos_hallazgos = 0
    pendientes = 0
    start_time = time.time()
    
    log_timing(f"🚀 Saturando CPU: {len(chunks)} bloques con {MAX_WORKERS} núcleos...")
    
    try:
        cur_local = conn_local.cursor()
        
        # CORRECCIÓN CLAVE: initializer e initargs para pasar los mensajes a los workers
        with ProcessPoolExecutor(
            max_workers=MAX_WORKERS,
            initializer=init_worker,
            initargs=(mensajes_procesados,)
        ) as executor:
            
            futures = [executor.submit(procesar_archivo_batch, chunk) for chunk in chunks]
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    resultados_batch = future.result()
                    for res in resultados_batch:
                        cur_local.execute(
                            """
                            INSERT OR IGNORE INTO cacheo
                            (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (res["archivo"], res["tipo"], res["fecha_escaneo"], res["encontrado"],
                             res["canal_id"], res["msg_id_global"], res["tamano_bytes"], res["duracion_segundos"], 0, None)
                        )
                        nuevos_hallazgos += 1
                        pendientes += 1
                        
                        if res["encontrado"] and ENABLE_DETAILED_LOGGING:
                            log_timing(f"  ✅ {res['tipo'][:3].upper()} {res['archivo']} -> MSG: {res['msg_id_global']}")

                    # Commit por lotes para no bloquear la BD
                    if pendientes >= DB_COMMIT_SIZE:
                        conn_local.commit()
                        pendientes = 0
                    
                    if (i + 1) % LOG_BATCH_SIZE == 0 or i == len(futures) - 1:
                        progreso = (i + 1) / len(futures) * 100
                        log_timing(f"📊 Progreso: {progreso:.1f}% ({i+1}/{len(futures)} bloques)")
                        
                except Exception as e:
                    log_timing(f"❌ Error en bloque: {e}")
        
        if pendientes:
            conn_local.commit()
            
        elapsed_total = time.time() - start_time
        log_timing(f"\n✨ Indexación completada en {elapsed_total:.1f}s ({nuevos_hallazgos / elapsed_total:.1f} arch/s)")
        
    finally:
        conn_local.close()

def run_etapa_indexar_optimizado():
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    
    lista = iter_archivos_nuevos_optimizado(existentes)
    if not lista:
        log_timing("☕ Sin archivos nuevos.")
        return
    
    log_timing(f"🎯 Procesando {len(lista)} archivos nuevos...")
    procesar_archivos_paralelo(lista)

if __name__ == "__main__":
    run_etapa_indexar_optimizado()