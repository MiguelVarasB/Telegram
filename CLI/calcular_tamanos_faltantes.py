"""
CALCULAR TAMAÑOS DE ARCHIVOS FALTANTES
=====================================

Este script busca en la tabla 'cacheo' todos los registros que no tienen
tamano_bytes (NULL o 0) y calcula el tamaño real del archivo en disco.

Funcionalidad:
1. Escanea la tabla cacheo en busca de registros sin tamano_bytes
2. Verifica si el archivo existe físicamente en disco
3. Calcula el tamaño real del archivo usando os.path.getsize()
4. Actualiza la base de datos con el tamaño calculado
5. Proporciona estadísticas del proceso

Uso:
    python calcular_tamanos_faltantes.py
"""

import os
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple, Optional

# Agregar el path del proyecto para importar utils y configuración
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import log_timing
from unigram_cacheo.common import CARPETAS, DB_LOCAL

def obtener_registros_sin_tamano() -> List[Tuple[int, str, str]]:
    """
    Obtiene todos los registros de la tabla cacheo que no tienen tamano_bytes.
    
    Returns:
        Lista de tuplas (id, archivo, tipo) de registros sin tamaño
    """
    conn = sqlite3.connect(DB_LOCAL)
    cursor = conn.cursor()
    
    # Buscar registros donde tamano_bytes es NULL o 0
    cursor.execute("""
        SELECT id, archivo, tipo 
        FROM cacheo 
        WHERE tamano_bytes IS NULL OR tamano_bytes = 0
        ORDER BY tipo, archivo
    """)
    
    registros = cursor.fetchall()
    conn.close()
    
    return registros

def encontrar_ruta_archivo(nombre_archivo: str, tipo_archivo: str) -> Optional[str]:
    """
    Busca la ruta completa de un archivo en las carpetas configuradas.
    
    Args:
        nombre_archivo: Nombre del archivo a buscar
        tipo_archivo: Tipo de archivo (video, thumbnail, etc.)
        
    Returns:
        Ruta completa del archivo o None si no se encuentra
    """
    # Buscar en la carpeta correspondiente según el tipo
    if tipo_archivo in CARPETAS:
        ruta_carpeta = CARPETAS[tipo_archivo]
        ruta_completa = os.path.join(ruta_carpeta, nombre_archivo)
        
        if os.path.exists(ruta_completa):
            return ruta_completa
    
    # Si no se encuentra en la carpeta específica, buscar en todas las carpetas
    for tipo, ruta in CARPETAS.items():
        ruta_completa = os.path.join(ruta, nombre_archivo)
        if os.path.exists(ruta_completa):
            return ruta_completa
    
    return None

def calcular_tamano_archivo(ruta_archivo: str) -> Optional[int]:
    """
    Calcula el tamaño de un archivo en bytes.
    
    Args:
        ruta_archivo: Ruta completa del archivo
        
    Returns:
        Tamaño en bytes o None si hay error
    """
    try:
        return os.path.getsize(ruta_archivo)
    except (OSError, IOError) as e:
        log_timing(f"Error al obtener tamaño de {ruta_archivo}: {e}")
        return None

def actualizar_tamano_en_bd(registro_id: int, tamano: int) -> bool:
    """
    Actualiza el tamaño de un archivo en la base de datos.
    
    Args:
        registro_id: ID del registro en la tabla cacheo
        tamano: Tamaño en bytes del archivo
        
    Returns:
        True si se actualizó correctamente, False si no
    """
    try:
        conn = sqlite3.connect(DB_LOCAL)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE cacheo 
            SET tamano_bytes = ? 
            WHERE id = ?
        """, (tamano, registro_id))
        
        conn.commit()
        conn.close()
        return True
        
    except sqlite3.Error as e:
        log_timing(f"Error actualizando BD para registro {registro_id}: {e}")
        return False

def procesar_registros_pendientes() -> None:
    """
    Función principal que procesa todos los registros sin tamaño.
    """
    log_timing("🔍 Buscando registros sin tamano_bytes en tabla cacheo...")
    
    # Obtener registros pendientes
    registros_pendientes = obtener_registros_sin_tamano()
    
    if not registros_pendientes:
        log_timing("✅ No hay registros sin tamano_bytes. Todos los archivos ya tienen tamaño.")
        return
    
    log_timing(f"📊 Se encontraron {len(registros_pendientes)} registros sin tamano_bytes")
    
    # Estadísticas
    actualizados = 0
    no_encontrados = 0
    errores = 0
    tamano_total = 0
    
    # Procesar cada registro
    for i, (registro_id, nombre_archivo, tipo_archivo) in enumerate(registros_pendientes, 1):
        # Mostrar progreso cada 50 archivos
        if i % 50 == 0 or i == len(registros_pendientes):
            log_timing(f"🔄 Procesando {i}/{len(registros_pendientes)}: {nombre_archivo}")
        
        # Buscar ruta del archivo
        ruta_archivo = encontrar_ruta_archivo(nombre_archivo, tipo_archivo)
        
        if not ruta_archivo:
            log_timing(f"❌ Archivo no encontrado: {nombre_archivo} (tipo: {tipo_archivo})")
            no_encontrados += 1
            continue
        
        # Calcular tamaño
        tamano = calcular_tamano_archivo(ruta_archivo)
        
        if tamano is None:
            log_timing(f"❌ Error al calcular tamaño: {ruta_archivo}")
            errores += 1
            continue
        
        # Actualizar base de datos
        if actualizar_tamano_en_bd(registro_id, tamano):
            actualizados += 1
            tamano_total += tamano
            
            # Mostrar éxito para archivos grandes (>10MB)
            if tamano > 10 * 1024 * 1024:  # 10MB
                tamano_mb = tamano / (1024 * 1024)
                log_timing(f"✅ Actualizado: {nombre_archivo} - {tamano_mb:.1f} MB")
        else:
            errores += 1
    
    # Mostrar resumen final
    log_timing("\n" + "="*60)
    log_timing("📈 RESUMEN DEL PROCESAMIENTO")
    log_timing("="*60)
    log_timing(f"📊 Total registros procesados: {len(registros_pendientes)}")
    log_timing(f"✅ Archivos actualizados: {actualizados}")
    log_timing(f"❌ Archivos no encontrados: {no_encontrados}")
    log_timing(f"❌ Errores al procesar: {errores}")
    
    if tamano_total > 0:
        tamano_mb = tamano_total / (1024 * 1024)
        tamano_gb = tamano_mb / 1024
        if tamano_gb >= 1:
            log_timing(f"💾 Tamaño total procesado: {tamano_gb:.2f} GB")
        else:
            log_timing(f"💾 Tamaño total procesado: {tamano_mb:.1f} MB")
    
    # Recomendaciones
    if no_encontrados > 0:
        log_timing(f"\n⚠️ ADVERTENCIA: {no_encontrados} archivos no fueron encontrados en disco.")
        log_timing("   Estos archivos pueden haber sido eliminados o movidos.")
    
    if errores > 0:
        log_timing(f"\n⚠️ ADVERTENCIA: {errores} archivos no pudieron ser procesados.")
        log_timing("   Revise los permisos de archivo y el acceso a las carpetas.")
    
    if actualizados > 0:
        log_timing(f"\n🎯 ÉXITO: Se actualizaron {actualizados} registros con sus tamaños reales.")
    else:
        log_timing("\nℹ️ INFO: No se realizaron actualizaciones.")

def mostrar_estadisticas_iniciales():
    """Muestra estadísticas iniciales de la tabla cacheo."""
    conn = sqlite3.connect(DB_LOCAL)
    cursor = conn.cursor()
    
    # Total de registros
    cursor.execute("SELECT COUNT(*) FROM cacheo")
    total_registros = cursor.fetchone()[0]
    
    # Registros con tamaño
    cursor.execute("SELECT COUNT(*) FROM cacheo WHERE tamano_bytes IS NOT NULL AND tamano_bytes > 0")
    con_tamano = cursor.fetchone()[0]
    
    # Registros sin tamaño
    sin_tamano = total_registros - con_tamano
    
    conn.close()
    
    log_timing("📊 ESTADÍSTICAS INICIALES")
    log_timing("="*40)
    log_timing(f"📁 Total registros en cacheo: {total_registros}")
    log_timing(f"✅ Con tamano_bytes: {con_tamano}")
    log_timing(f"❌ Sin tamano_bytes: {sin_tamano}")
    
    if sin_tamano > 0:
        porcentaje = (sin_tamano / total_registros) * 100
        log_timing(f"📈 Porcentaje sin tamaño: {porcentaje:.1f}%")
    
    return sin_tamano > 0

def main():
    """
    Función principal del script.
    """
    print("🔧 CALCULAR TAMAÑOS DE ARCHIVOS FALTANTES")
    print("="*50)
    
    # Verificar que la base de datos exista
    if not os.path.exists(DB_LOCAL):
        log_timing(f"❌ Error: No se encuentra la base de datos en {DB_LOCAL}")
        return
    
    # Mostrar estadísticas iniciales
    hay_pendientes = mostrar_estadisticas_iniciales()
    
    if not hay_pendientes:
        log_timing("\n✅ No hay archivos pendientes de procesar.")
        return
    
    # Confirmar procesamiento
    print(f"\n🚀 Se procesarán los archivos sin tamaño.")
    print("¿Desea continuar? (S/N): ", end="")
    
    try:
        respuesta = input().strip().upper()
        if respuesta not in ['S', 'SI', 'Y', 'YES']:
            log_timing("❌ Proceso cancelado por el usuario.")
            return
    except KeyboardInterrupt:
        log_timing("\n❌ Proceso cancelado por el usuario.")
        return
    
    # Procesar registros pendientes
    procesar_registros_pendientes()
    
    log_timing("\n🎉 Proceso completado.")

if __name__ == "__main__":
    main()
