"""
ETAPA INDEXAR - Módulo de indexación de archivos multimedia de Telegram
========================================================================

Este archivo es responsable de escanear, indexar y relacionar archivos multimedia 
(descargados de Telegram) con sus mensajes originales en la base de datos de Unigram.

FUNCIONALIDADES PRINCIPALES:
----------------------------
1. Escanea carpetas en busca de archivos nuevos (imágenes, videos, .bin)
2. Extrae IDs numéricos de los nombres de archivo (patrón 15-20 dígitos)
3. Busca coincidencias en la base de datos cifrada de Unigram mediante:
   - Búsqueda binaria en blobs de datos (método principal)
   - Búsqueda por nombre de archivo (fallback)
4. Almacena metadatos en tabla local 'cacheo': tamaño, duración, canal_id, msg_id
5. Maneja archivos sin patrón numérico (videos sin ID)

FLUJO DE TRABAJO:
------------------
1. iter_archivos_nuevos() → Identifica archivos no procesados
2. procesar_archivos() → Procesa cada archivo y lo relaciona con mensajes
3. run_etapa_indexar() → Punto de entrada principal

BASES DE DATOS UTILIZADAS:
--------------------------
- DB_UNIGRAM (sqlcipher): Base cifrada de Unigram con mensajes originales
- DB local (cacheo.sqlite): Tabla 'cacheo' con resultados de indexación
"""

import os
import struct
import re
import datetime
from typing import Iterable

from sqlcipher3 import dbapi2 as sqlcipher

from .common import (
    CARPETAS,
    MASTER_KEY,
    DB_UNIGRAM,
    preparar_base_local,
    obtener_duracion_video,
    cargar_mensajes_unigram,
)
from utils import  log_timing

def iter_archivos_nuevos(archivos_ya_indexados: set) -> list[dict]:
    """
    Escanea las carpetas configuradas en busca de archivos nuevos que no han sido indexados.
    
    Args:
        archivos_ya_indexados: Conjunto de nombres de archivos ya procesados
        
    Returns:
        Lista de diccionarios con información de archivos nuevos, ordenados por fecha de creación (más nuevos primero)
        
    Estructura del diccionario devuelto:
        {
            "nombre": str,           # Nombre del archivo
            "tipo": str,            # Tipo según CARPETAS (ej: "video", "imagen")
            "ruta": str,            # Ruta completa al archivo
            "fecha_creacion": float # Timestamp de creación
        }
    """
    # Extensiones de archivos multimedia que Telegram descarga
    extensiones = ('.jpg', '.png', '.mp4', '.m4v', '.mov', '.bin')
    lista = []
    
    # Recorrer todas las carpetas configuradas en common.py
    for tipo, ruta in CARPETAS.items():
        if not os.path.exists(ruta):
            continue  # Saltar carpetas que no existen
            
        # Escanear archivos en la carpeta
        for f in os.listdir(ruta):
            # Incluir: archivos con extensiones válidas O archivos que son solo números (IDs)
            if f.lower().endswith(extensiones) or f.isdigit():
                # Procesar solo si no está indexado
                if f not in archivos_ya_indexados:
                    full_path = os.path.join(ruta, f)
                    lista.append(
                        {
                            "nombre": f,
                            "tipo": tipo,
                            "ruta": full_path,
                            "fecha_creacion": os.path.getctime(full_path),
                        }
                    )
    
    # Ordenar por fecha de creación (más nuevos primero) para priorizar contenido reciente
    lista.sort(key=lambda x: x["fecha_creacion"], reverse=True)
    return lista


def _buscar_en_unigram_por_nombre(nombre_archivo: str, cur_unigram) -> tuple[int | None, int | None]:
    """
    MÉTODO FALLBACK: Busca un archivo en la base de datos de Unigram por su nombre exacto.
    
    Este método se utiliza cuando la búsqueda binaria por ID no encuentra coincidencias.
    Busca el nombre del archivo dentro de los blobs de datos de los mensajes.
    
    Args:
        nombre_archivo: Nombre del archivo a buscar
        cur_unigram: Cursor de la base de datos de Unigram ya abierta
        
    Returns:
        Tupla (dialog_id, message_id_real) si encuentra coincidencia, o (None, None) si no
        
    Nota:
        - message_id_real = message_id // 1048576 (conversión de ID interno de Unigram)
        - Este método es más lento que la búsqueda binaria pero útil como fallback
    """
    nombre_bytes = nombre_archivo.encode("utf-8")
    
    # Buscar en todos los mensajes que tengan datos binarios
    cur_unigram.execute("SELECT dialog_id, message_id, data FROM messages WHERE data IS NOT NULL")
    
    for d_id, m_id, blob in cur_unigram:
        # Verificar si el nombre del archivo está contenido en el blob de datos
        if nombre_bytes in blob:
            # Convertir el ID de mensaje de Unigram a ID global real
            msg_id_real = m_id // 1048576
            return d_id, msg_id_real
            
    return None, None


def procesar_archivos(lista_a_procesar: Iterable[dict]) -> None:
    """
    FUNCIÓN PRINCIPAL: Procesa una lista de archivos y los relaciona con mensajes de Telegram.
    
    Esta función realiza el trabajo pesado de indexación:
    1. Extrae IDs numéricos de los nombres de archivo
    2. Busca coincidencias en la base de datos de Unigram
    3. Extrae metadatos (tamaño, duración para videos)
    4. Guarda resultados en la tabla local 'cacheo'
    5. Aplica fallback por nombre si la búsqueda binaria falla
    
    Args:
        lista_a_procesar: Iterable de diccionarios con información de archivos (de iter_archivos_nuevos)
        
    Proceso detallado:
        - Para cada archivo: extraer ID → buscar en blobs → guardar metadatos → fallback si es necesario
        - Usa commits en lotes de 100 para optimizar rendimiento
        - Maneja archivos sin patrón numérico (videos sin ID)
    """
    # Preparar conexión a base de datos local y obtener archivos ya indexados
    conn_local, archivos_ya_indexados = preparar_base_local()
    nuevos_hallazgos = 0
    batch_size = 100  # Tamaño del lote para commits a BD
    pendientes = 0
    saltados_patron: list[str] = []  # Archivos que no cumplen el patrón de ID numérico

    # Cargar todos los mensajes cifrados de Unigram en memoria para búsqueda eficiente
    conn_unigram, todos_los_mensajes = cargar_mensajes_unigram()
    log_timing(f"✅ {len(todos_los_mensajes)} mensajes cargados.")

    # Conexión adicional para fallback por nombre (se mantiene abierta para múltiples búsquedas)
    conn_uni_nom = sqlcipher.connect(DB_UNIGRAM)
    cur_uni_nom = conn_uni_nom.cursor()
    cur_uni_nom.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")  # Descifrar BD
    cur_uni_nom.execute("PRAGMA cipher_compatibility = 4;")    # Compatibilidad con versión antigua

    try:
        cur_local = conn_local.cursor()
        total_items = len(lista_a_procesar)
        
        # Procesar cada archivo en la lista
        for idx, item in enumerate(lista_a_procesar, start=1):
            nombre_f = item["nombre"]
            
            # Mostrar progreso cada 50 archivos o al final
            if idx % 50 == 0 or idx == total_items:
                log_timing(f"🔎 Procesando {idx}/{total_items}: {nombre_f}")

            # EXTRAER ID NUMÉRICO: Buscar número de 15-20 dígitos en el nombre del archivo
            # Este ID es la huella digital que permite relacionar el archivo con su mensaje
            match = re.search(r"(\d{15,20})", nombre_f)
            if not match:
                # CASO 1: Archivo sin ID numérico (videos descargados sin número)
                if item["tipo"] == "video":
                    # Extraer metadatos básicos del video
                    tamano_bytes = os.path.getsize(item["ruta"])
                    duracion_segundos = obtener_duracion_video(item["ruta"])
                    
                    # Guardar en BD como video no relacionado (encontrado=0)
                    cur_local.execute(
                        """
                        INSERT OR IGNORE INTO cacheo
                        (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            nombre_f,
                            item["tipo"],
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            0,          # encontrado=0 (no se encontró relación)
                            None,       # canal_id desconocido
                            None,       # msg_id_global desconocido
                            tamano_bytes,
                            duracion_segundos,
                            0,          # en_servidor=0
                            None,       # unique_id desconocido
                        ),
                    )
                    pendientes += 1
                    nuevos_hallazgos += 1
                    
                    # Commit parcial cada batch_size registros
                    if pendientes >= batch_size:
                        conn_local.commit()
                        log_timing(f"💾 Guardados {nuevos_hallazgos} registros (commit parcial)...")
                        pendientes = 0
                else:
                    # Archivos no videos sin ID se omiten (imágenes sin número no son útiles)
                    saltados_patron.append(nombre_f)
                continue

            # CASO 2: Archivo con ID numérico - procesamiento normal
            id_cache_num = int(match.group(1))
            huella_bin = struct.pack("<q", id_cache_num)  # Convertir ID a binario little-endian

            # Extraer metadatos del archivo
            tamano_bytes = os.path.getsize(item["ruta"])
            duracion_segundos = None
            if item["tipo"] == "video":
                duracion_segundos = obtener_duracion_video(item["ruta"])

            # Estructura de información a guardar en BD
            info = {
                "archivo": nombre_f,
                "tipo": item["tipo"],
                "fecha_escaneo": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "encontrado": 0,        # Por defecto no encontrado
                "canal_id": None,
                "msg_id_global": None,
                "tamano_bytes": tamano_bytes,
                "duracion_segundos": duracion_segundos,
            }

            # BÚSQUEDA PRINCIPAL: Buscar la huella binaria en los mensajes de Unigram
            for m_id, d_id, data_blob in todos_los_mensajes:
                if data_blob and huella_bin in data_blob:
                    # ¡Coincidencia encontrada! Actualizar información
                    info["encontrado"] = 1
                    info["canal_id"] = d_id
                    info["msg_id_global"] = m_id // 1048576  # Convertir a ID real
                    break

            # Guardar en base de datos local
            cur_local.execute(
                """
                INSERT OR IGNORE INTO cacheo
                (archivo, tipo, fecha_escaneo, encontrado, canal_id, msg_id_global, tamano_bytes, duracion_segundos, en_servidor, unique_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    info["archivo"],
                    info["tipo"],
                    info["fecha_escaneo"],
                    info["encontrado"],
                    info["canal_id"],
                    info["msg_id_global"],
                    info["tamano_bytes"],
                    info["duracion_segundos"],
                    0,          # en_servidor=0
                    None,       # unique_id desconocido
                ),
            )
            pendientes += 1
            nuevos_hallazgos += 1

            # Commit parcial cada batch_size registros
            if pendientes >= batch_size:
                conn_local.commit()
                log_timing(f"💾 Guardados {nuevos_hallazgos} registros (commit parcial)...")
                pendientes = 0

            # Mostrar éxito si se encontró relación
            if info["encontrado"]:
                log_timing(
                    f"  ✅ {info['tipo'][:3].upper()} {nombre_f} -> Global: {info['msg_id_global']} (Canal: {info['canal_id']})"
                )
            
            # FALLBACK: Si es video y no se encontró por blob, intentar búsqueda por nombre
            if info["tipo"] == "video" and not info["encontrado"]:
                chat_id_f, msg_id_f = _buscar_en_unigram_por_nombre(nombre_f, cur_uni_nom)
                if chat_id_f is not None and msg_id_f is not None:
                    # Actualizar el registro con la información encontrada por fallback
                    cur_local.execute(
                        """
                        UPDATE cacheo
                        SET encontrado = 1, canal_id = ?, msg_id_global = ?
                        WHERE archivo = ?
                        """,
                        (chat_id_f, msg_id_f, info["archivo"]),
                    )
                    info["encontrado"] = 1
                    info["canal_id"] = chat_id_f
                    info["msg_id_global"] = msg_id_f
                    log_timing(f"  ✅ Fallback nombre -> Global: {msg_id_f} (Canal: {chat_id_f}) para {nombre_f}")

        # Commit final de los registros pendientes
        if pendientes:
            conn_local.commit()

        # Resumen final del procesamiento
        log_timing(f"\n✨ ¡Hecho! Se agregaron {nuevos_hallazgos} entradas a cacheo (ignorando duplicados).")
        if saltados_patron:
            log_timing(f"⚠️ Saltados por no cumplir patrón (sin número 15-20 dígitos): {len(saltados_patron)}")
            for name in saltados_patron[:30]:  # Mostrar primeros 30
                log_timing(f"   - {name}")
            if len(saltados_patron) > 30:
                log_timing("   ...")
                
    finally:
        # Cerrar todas las conexiones a bases de datos
        conn_local.close()
        conn_unigram.close()
        conn_uni_nom.close()


def run_etapa_indexar():
    """
    PUNTO DE ENTRADA PRINCIPAL del módulo de indexación.
    
    Esta función orquesta todo el proceso de indexación:
    1. Verifica archivos ya procesados en la base de datos local
    2. Identifica archivos nuevos en las carpetas configuradas
    3. Inicia el procesamiento de los archivos encontrados
    
    Es la función que se debe llamar para ejecutar una nueva ronda de indexación.
    """
    # Obtener conexión y lista de archivos ya indexados
    conn_local, existentes = preparar_base_local()
    conn_local.close()
    
    # Identificar archivos nuevos que no han sido procesados
    lista = iter_archivos_nuevos(existentes)
    
    if not lista:
        log_timing("☕ No hay archivos nuevos que procesar.")
        return
        
    # Iniciar procesamiento de los archivos nuevos
    log_timing(f"🚀 Analizando {len(lista)} archivos nuevos...")
    procesar_archivos(lista)


if __name__ == "__main__":
    """
    Permite ejecutar este módulo directamente desde línea de comandos:
    python etapa_indexar.py
    """
    run_etapa_indexar()
