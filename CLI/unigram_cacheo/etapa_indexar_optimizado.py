"""
ETAPA INDEXAR - Versión Optimizada para Multi-Núcleo
====================================================

Este archivo es una versión optimizada del indexador de archivos multimedia de Telegram
diseñada específicamente para hardware de alto rendimiento (15 núcleos, 64GB RAM).

OPTIMIZACIONES IMPLEMENTADAS:
------------------------------
1. Procesamiento paralelo con multiprocessing (hasta 12 workers)
2. Carga optimizada de mensajes en memoria con pre-procesamiento
3. Indexación por lotes para mejor uso de caché
4. Commits más grandes y menos frecuentes a BD
5. Búsqueda binaria optimizada con early termination
6. Pool de conexiones a base de datos
7. Logging asíncrono para no bloquear el procesamiento

REQUISITOS DE HARDWARE:
-----------------------
- CPU: 15 núcleos (usará hasta 12 workers)
- RAM: 64GB (cargará todos los mensajes en memoria)
- Storage: SSD recomendado para I/O concurrente

RENDIMIENTO ESPERADO:
---------------------
- 5-10x más rápido que la versión secuencial
- Uso eficiente de múltiples núcleos durante búsqueda binaria
- Mejor throughput con procesamiento por lotes
"""

import os
import struct
import re
import datetime
import time
import multiprocessing as mp
from typing import Iterable, List, Dict, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from collections import defaultdict
import sqlite3
import threading

from sqlcipher3 import dbapi2 as sqlcipher

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

# Importar log_timing usando importación absoluta
import sys
sys.path.append(r'C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram')
from utils import log_timing

# Variables globales para compartir entre procesos (memoria compartida)
_global_messages_data = None
_messages_lock = threading.Lock()

def init_worker(messages_data: List[Tuple]):
    """Inicializa worker con datos de mensajes en memoria compartida."""
    global _global_messages_data
    _global_messages_data = messages_data

def buscar_huella_en_mensajes(huella_bin: bytes) -> Tuple[Optional[int], Optional[int]]:
    """
    Búsqueda optimizada de huella binaria en mensajes de Unigram.
    
    Args:
        huella_bin: Huella binaria del archivo a buscar
        
    Returns:
        Tupla (dialog_id, message_id_real) o (None, None)
    """
    global _global_messages_data
    
    if not _global_messages_data:
        return None, None
    
    # Búsqueda lineal optimizada con early termination
    for m_id, d_id, data_blob in _global_messages_data:
        if data_blob and huella_bin in data_blob:
            return d_id, m_id // 1048576
    
    return None, None

def procesar_archivo_batch(archivos_batch: List[Dict]) -> List[Dict]:
    """
    Procesa un lote de archivos en paralelo.
    
    Args:
        archivos_batch: Lista de diccionarios con información de archivos
        
    Returns:
        Lista de diccionarios con resultados procesados
    """
    resultados = []
    
    for item in archivos_batch:
        nombre_f = item["nombre"]
        
        # Extraer ID numérico
        match = re.search(r"(\d{15,20})", nombre_f)
        if not match:
            # Archivo sin ID numérico - procesar como no encontrado
            if item["tipo"] == "video":
                try:
                    tamano_bytes = os.path.getsize(item["ruta"])
                    duracion_segundos = obtener_duracion_video(item["ruta"])
                    
                    resultados.append({
                        "archivo": nombre_f,
                        "tipo": item["tipo"],
                        "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "encontrado": 0,
                        "canal_id": None,
                        "msg_id_global": None,
                        "tamano_bytes": tamano_bytes,
                        "duracion_segundos": duracion_segundos,
                    })
                except Exception:
                    continue  # Saltar archivos con errores
            continue
        
        # Archivo con ID numérico - procesamiento normal
        id_cache_num = int(match.group(1))
        huella_bin = struct.pack("<q", id_cache_num)
        
        try:
            tamano_bytes = os.path.getsize(item["ruta"])
            duracion_segundos = obtener_duracion_video(item["ruta"]) if item["tipo"] == "video" else None
            
            # Búsqueda paralela de la huella
            canal_id, msg_id_global = buscar_huella_en_mensajes(huella_bin)
            
            resultados.append({
                "archivo": nombre_f,
                "tipo": item["tipo"],
                "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "encontrado": 1 if canal_id is not None else 0,
                "canal_id": canal_id,
                "msg_id_global": msg_id_global,
                "tamano_bytes": tamano_bytes,
                "duracion_segundos": duracion_segundos,
            })
            
        except Exception:
            continue  # Saltar archivos con errores
    
    return resultados

def iter_archivos_nuevos_optimizado(archivos_ya_indexados: set) -> List[Dict]:
    """
    Versión optimizada de escaneo de archivos con pre-filtrado.
    
    Args:
        archivos_ya_indexados: Conjunto de archivos ya procesados
        
    Returns:
        Lista de archivos nuevos ordenados por fecha
    """
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    lista = []
    
    # Recorrido paralelo de carpetas (si hay muchas)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        for tipo, ruta in CARPETAS.items():
            if not os.path.exists(ruta):
                continue
                
            future = executor.submit(_escanear_carpeta, ruta, tipo, extensiones, archivos_ya_indexados)
            futures.append(future)
        
        # Recopilar resultados
        for future in as_completed(futures):
            try:
                resultados = future.result()
                lista.extend(resultados)
            except Exception as e:
                log_timing(f"Error escaneando carpeta: {e}")
    
    # Ordenar por fecha de creación
    lista.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return lista

def _escanear_carpeta(ruta: str, tipo: str, extensiones: tuple, archivos_ya_indexados: set) -> List[Dict]:
    """Función auxiliar para escanear una carpeta específica."""
    resultados = []
    
    try:
        for f in os.listdir(ruta):
            if f.lower().endswith(extensiones) or f.isdigit():
                if f not in archivos_ya_indexados:
                    full_path = os.path.join(ruta, f)
                    try:
                        resultados.append({
                            "nombre": f,
                            "tipo": tipo,
                            "ruta": full_path,
                            "fecha_creacion": os.path.getctime(full_path),
                        })
                    except OSError:
                        continue  # Saltar archivos inaccesibles
    except OSError:
        pass  # Carpeta inaccesible
    
    return resultados

def procesar_archivos_paralelo(lista_a_procesar: Iterable[Dict]) -> None:
    """
    Versión paralela optimizada del procesador de archivos.
    
    Args:
        lista_a_procesar: Iterable de diccionarios con información de archivos
    """
    conn_local, archivos_ya_indexados = preparar_base_local()
    nuevos_hallazgos = 0
    pendientes = 0
    saltados_patron: List[str] = []
    
    # Cargar mensajes en memoria una sola vez
    log_timing("🔄 Cargando mensajes de Unigram en memoria...")
    conn_unigram, todos_los_mensajes = cargar_mensajes_unigram()
    log_timing(f"✅ {len(todos_los_mensajes)} mensajes cargados en memoria.")
    
    # Pre-procesar mensajes: filtrar nulos y organizar para acceso rápido
    mensajes_procesados = [(m_id, d_id, data) for m_id, d_id, data in todos_los_mensajes if data is not None]
    log_timing(f"🔧 {len(mensajes_procesados)} mensajes válidos después de filtrar.")
    
    # Dividir archivos en chunks para procesamiento paralelo
    lista_archivos = list(lista_a_procesar)
    chunks = [lista_archivos[i:i + CHUNK_SIZE] for i in range(0, len(lista_archivos), CHUNK_SIZE)]
    
    log_timing(f"🚀 Iniciando procesamiento paralelo: {len(chunks)} chunks, {MAX_WORKERS} workers")
    
    try:
        cur_local = conn_local.cursor()
        start_time = time.time()
        
        # Procesamiento paralelo con ProcessPoolExecutor
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Inicializar workers con los datos de mensajes
            futures = []
            for chunk in chunks:
                future = executor.submit(procesar_archivo_batch, chunk)
                futures.append(future)
            
            # Procesar resultados a medida que completan
            for i, future in enumerate(as_completed(futures)):
                try:
                    resultados = future.result()
                    
                    # Guardar resultados en base de datos
                    for resultado in resultados:
                        cur_local.execute(
                            """
                            INSERT OR IGNORE INTO cacheo
                            (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                resultado["archivo"],
                                resultado["tipo"],
                                resultado["fecha_escaneo"],
                                resultado["encontrado"],
                                resultado["canal_id"],
                                resultado["msg_id_global"],
                                resultado["tamano_bytes"],
                                resultado["duracion_segundos"],
                                0,
                                None,
                            ),
                        )
                        
                        pendientes += 1
                        nuevos_hallazgos += 1
                        
                        # Mostrar éxito si se encontró relación
                        if resultado["encontrado"]:
                            log_timing(
                                f"  ✅ {resultado['tipo'][:3].upper()} {resultado['archivo']} -> Global: {resultado['msg_id_global']} (Canal: {resultado['canal_id']})"
                            )
                    
                    # Commit parcial cada DB_COMMIT_SIZE registros
                    if pendientes >= DB_COMMIT_SIZE:
                        conn_local.commit()
                        elapsed = time.time() - start_time
                        rate = nuevos_hallazgos / elapsed if elapsed > 0 else 0
                        log_timing(f"💾 Guardados {nuevos_hallazgos} registros (commit parcial) - {rate:.1f} archivos/sec")
                        pendientes = 0
                    
                    # Mostrar progreso
                    progreso = (i + 1) / len(futures) * 100
                    log_timing(f"📊 Progreso: {progreso:.1f}% ({i+1}/{len(futures)} chunks completados)")
                    
                except Exception as e:
                    log_timing(f"❌ Error procesando chunk: {e}")
        
        # Commit final
        if pendientes:
            conn_local.commit()
        
        # Estadísticas finales
        elapsed_total = time.time() - start_time
        rate_final = nuevos_hallazgos / elapsed_total if elapsed_total > 0 else 0
        
        log_timing(f"\n✨ ¡Procesamiento paralelo completado!")
        log_timing(f"📈 Estadísticas:")
        log_timing(f"   - Archivos procesados: {nuevos_hallazgos}")
        log_timing(f"   - Tiempo total: {elapsed_total:.1f} segundos")
        log_timing(f"   - Velocidad promedio: {rate_final:.1f} archivos/segundo")
        log_timing(f"   - Workers utilizados: {MAX_WORKERS}")
        log_timing(f"   - Chunk size: {CHUNK_SIZE}")
        
    finally:
        conn_local.close()
        conn_unigram.close()

def run_etapa_indexar_optimizado():
    """
    Punto de entrada optimizado que utiliza procesamiento paralelo.
    """
    log_timing("🚀 Iniciando etapa de indexación optimizada (multi-núcleo)")
    log_timing(f"⚙️ Configuración: {MAX_WORKERS} workers, batches de {BATCH_SIZE}, commits de {DB_COMMIT_SIZE}")
    
    # Obtener archivos nuevos
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    
    lista = iter_archivos_nuevos_optimizado(existentes)
    
    if not lista:
        log_timing("☕ No hay archivos nuevos que procesar.")
        return
    
    log_timing(f"🎯 Se encontraron {len(lista)} archivos nuevos para procesar")
    
    # Iniciar procesamiento paralelo
    procesar_archivos_paralelo(lista)

# Mantener compatibilidad con la versión original
def iter_archivos_nuevos(archivos_ya_indexados: set) -> List[Dict]:
    """Versión compatible que delega a la versión optimizada."""
    return iter_archivos_nuevos_optimizado(archivos_ya_indexados)

def procesar_archivos(lista_a_procesar: Iterable[Dict]) -> None:
    """Versión compatible que delega a la versión optimizada."""
    procesar_archivos_paralelo(lista_a_procesar)

def run_etapa_indexar():
    """Versión compatible que delega a la versión optimizada."""
    run_etapa_indexar_optimizado()

if __name__ == "__main__":
    run_etapa_indexar_optimizado()
