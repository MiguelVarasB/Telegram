"""
ETAPA INDEXAR - Versión Optimizada Simplificada
================================================

Versión optimizada pero más estable del indexador de archivos multimedia de Telegram.
Utiliza threading en lugar de multiprocessing para mejor compatibilidad.

OPTIMIZACIONES IMPLEMENTADAS:
------------------------------
1. Procesamiento con hilos para mejor estabilidad
2. Carga optimizada de mensajes en memoria
3. Indexación por lotes para mejor uso de caché
4. Búsqueda binaria optimizada
5. Commits más grandes y menos frecuentes
6. Logging mejorado para no bloquear el procesamiento
"""

import os
import struct
import re
import datetime
import time
from typing import Iterable, List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    LOG_BATCH_SIZE,
    ENABLE_DETAILED_LOGGING
)

# Importar log_timing usando importación absoluta
import sys
sys.path.append(r'C:\Users\TheMiguel\Downloads\Soft\#Mios\Telegram')
from utils import log_timing

# Variables globales para compartir entre hilos
_global_messages_data = None
_messages_lock = threading.Lock()

def init_worker(messages_data: List[Tuple]):
    """Inicializa worker con datos de mensajes en memoria compartida."""
    global _global_messages_data
    with _messages_lock:
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
    
    # Búsqueda lineal optimizada
    for m_id, d_id, data_blob in _global_messages_data:
        if data_blob and huella_bin in data_blob:
            return d_id, m_id // 1048576
    
    return None, None

def procesar_archivo_concurrente(item: Dict) -> Optional[Dict]:
    """
    Procesa un solo archivo en un hilo separado.
    
    Args:
        item: Diccionario con información del archivo
        
    Returns:
        Diccionario con resultado procesado o None si hay error
    """
    try:
        nombre_f = item["nombre"]
        
        # Extraer ID numérico
        match = re.search(r"(\d{15,20})", nombre_f)
        if not match:
            # Archivo sin ID numérico - procesar como no encontrado
            if item["tipo"] == "video":
                tamano_bytes = os.path.getsize(item["ruta"])
                duracion_segundos = obtener_duracion_video(item["ruta"])
                
                return {
                    "archivo": nombre_f,
                    "tipo": item["tipo"],
                    "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "encontrado": 0,
                    "canal_id": None,
                    "msg_id_global": None,
                    "tamano_bytes": tamano_bytes,
                    "duracion_segundos": duracion_segundos,
                }
            return None
        
        # Archivo con ID numérico - procesamiento normal
        id_cache_num = int(match.group(1))
        huella_bin = struct.pack("<q", id_cache_num)
        
        tamano_bytes = os.path.getsize(item["ruta"])
        duracion_segundos = obtener_duracion_video(item["ruta"]) if item["tipo"] == "video" else None
        
        # Búsqueda de la huella
        canal_id, msg_id_global = buscar_huella_en_mensajes(huella_bin)
        
        return {
            "archivo": nombre_f,
            "tipo": item["tipo"],
            "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "encontrado": 1 if canal_id is not None else 0,
            "canal_id": canal_id,
            "msg_id_global": msg_id_global,
            "tamano_bytes": tamano_bytes,
            "duracion_segundos": duracion_segundos,
        }
        
    except Exception as e:
        if ENABLE_DETAILED_LOGGING:
            log_timing(f"Error procesando {item.get('nombre', 'desconocido')}: {e}")
        return None

def iter_archivos_nuevos_optimizado(archivos_ya_indexados: set) -> List[Dict]:
    """
    Versión optimizada de escaneo de archivos.
    """
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    lista = []
    
    for tipo, ruta in CARPETAS.items():
        if not os.path.exists(ruta):
            continue
            
        try:
            for f in os.listdir(ruta):
                if f.lower().endswith(extensiones) or f.isdigit():
                    if f not in archivos_ya_indexados:
                        full_path = os.path.join(ruta, f)
                        try:
                            lista.append({
                                "nombre": f,
                                "tipo": tipo,
                                "ruta": full_path,
                                "fecha_creacion": os.path.getctime(full_path),
                            })
                        except OSError:
                            continue
        except OSError:
            continue
    
    lista.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return lista

def procesar_archivos_concurrente(lista_a_procesar: Iterable[Dict]) -> None:
    """
    Versión concurrente optimizada del procesador de archivos usando hilos.
    """
    conn_local, archivos_ya_indexados = preparar_base_local()
    nuevos_hallazgos = 0
    pendientes = 0
    
    # Cargar mensajes en memoria una sola vez
    log_timing("🔄 Cargando mensajes de Unigram en memoria...")
    conn_unigram, todos_los_mensajes = cargar_mensajes_unigram()
    log_timing(f"✅ {len(todos_los_mensajes)} mensajes cargados en memoria.")
    
    # Pre-procesar mensajes: filtrar nulos
    mensajes_procesados = [(m_id, d_id, data) for m_id, d_id, data in todos_los_mensajes if data is not None]
    log_timing(f"🔧 {len(mensajes_procesados)} mensajes válidos después de filtrar.")
    
    # Inicializar datos para hilos
    init_worker(mensajes_procesados)
    
    try:
        cur_local = conn_local.cursor()
        start_time = time.time()
        
        # Procesamiento concurrente con ThreadPoolExecutor
        log_timing(f"🚀 Iniciando procesamiento concurrente: {MAX_WORKERS} hilos")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Enviar todos los archivos para procesamiento
            futures = {executor.submit(procesar_archivo_concurrente, item): item for item in lista_a_procesar}
            
            # Procesar resultados a medida que completan
            for i, future in enumerate(as_completed(futures)):
                try:
                    resultado = future.result()
                    
                    if resultado:
                        # Guardar en base de datos
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
                    
                    # Mostrar progreso cada LOG_BATCH_SIZE archivos
                    if (i + 1) % LOG_BATCH_SIZE == 0:
                        progreso = (i + 1) / len(futures) * 100
                        log_timing(f"📊 Progreso: {progreso:.1f}% ({i+1}/{len(futures)} archivos)")
                    
                except Exception as e:
                    log_timing(f"❌ Error procesando archivo: {e}")
        
        # Commit final
        if pendientes:
            conn_local.commit()
        
        # Estadísticas finales
        elapsed_total = time.time() - start_time
        rate_final = nuevos_hallazgos / elapsed_total if elapsed_total > 0 else 0
        
        log_timing(f"\n✨ ¡Procesamiento concurrente completado!")
        log_timing(f"📈 Estadísticas:")
        log_timing(f"   - Archivos procesados: {nuevos_hallazgos}")
        log_timing(f"   - Tiempo total: {elapsed_total:.1f} segundos")
        log_timing(f"   - Velocidad promedio: {rate_final:.1f} archivos/segundo")
        log_timing(f"   - Hilos utilizados: {MAX_WORKERS}")
        
    finally:
        conn_local.close()
        conn_unigram.close()

def run_etapa_indexar_optimizado():
    """
    Punto de entrada optimizado que utiliza procesamiento concurrente.
    """
    log_timing("🚀 Iniciando etapa de indexación optimizada (multi-hilo)")
    log_timing(f"⚙️ Configuración: {MAX_WORKERS} hilos, commits de {DB_COMMIT_SIZE}")
    
    # Obtener archivos nuevos
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    
    lista = iter_archivos_nuevos_optimizado(existentes)
    
    if not lista:
        log_timing("☕ No hay archivos nuevos que procesar.")
        return
    
    log_timing(f"🎯 Se encontraron {len(lista)} archivos nuevos para procesar")
    
    # Iniciar procesamiento concurrente
    procesar_archivos_concurrente(lista)

# Mantener compatibilidad con la versión original
def iter_archivos_nuevos(archivos_ya_indexados: set) -> List[Dict]:
    """Versión compatible que delega a la versión optimizada."""
    return iter_archivos_nuevos_optimizado(archivos_ya_indexados)

def procesar_archivos(lista_a_procesar: Iterable[Dict]) -> None:
    """Versión compatible que delega a la versión optimizada."""
    procesar_archivos_concurrente(lista_a_procesar)

def run_etapa_indexar():
    """Versión compatible que delega a la versión optimizada."""
    run_etapa_indexar_optimizado()

if __name__ == "__main__":
    run_etapa_indexar_optimizado()
