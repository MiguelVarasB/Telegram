"""
Gestor de Caché Inteligente en Disco (LRU).
Administra una carpeta con un límite de tamaño (ej. 2GB).
Si se llena, elimina los archivos menos usados recientemente.
"""
import os
import time
import shutil
import aiofiles
from config import CACHE_DIR, MAX_DISK_CACHE_SIZE, SMART_CACHE_ENABLED

def get_cache_path(video_id: str) -> str:
    """Retorna la ruta física donde debería estar el video."""
    return os.path.join(CACHE_DIR, f"{video_id}.cache")

def _get_directory_size(directory):
    """Calcula el tamaño total de la carpeta en bytes."""
    total = 0
    try:
        for entry in os.scandir(directory):
            if entry.is_file():
                total += entry.stat().st_size
    except OSError:
        pass
    return total

def _enforce_limit(new_bytes_needed: int):
    """
    Algoritmo de limpieza (LRU Eviction).
    Si (actual + nuevo) > limite, borra archivos viejos (por fecha de acceso).
    """
    if not SMART_CACHE_ENABLED:
        return
    current_size = _get_directory_size(CACHE_DIR)
    
    if current_size + new_bytes_needed <= MAX_DISK_CACHE_SIZE:
        return # Hay espacio, todo bien.

    print(f"🧹 [SmartCache] Límite excedido ({current_size/1024/1024:.1f}MB). Liberando espacio...")

    # 1. Listar archivos con su fecha de último acceso (atime)
    files = []
    for entry in os.scandir(CACHE_DIR):
        if entry.is_file():
            files.append((entry.path, entry.stat().st_atime, entry.stat().st_size))
    
    # 2. Ordenar: los más viejos primero (menor atime)
    files.sort(key=lambda x: x[1])

    # 3. Borrar hasta hacer espacio
    freed = 0
    for path, atime, size in files:
        try:
            os.remove(path)
            freed += size
            current_size -= size
            # print(f"🗑️ Eliminado: {os.path.basename(path)}")
            
            if current_size + new_bytes_needed <= MAX_DISK_CACHE_SIZE:
                break
        except OSError as e:
            print(f"⚠️ Error borrando {path}: {e}")
            
    print(f"✨ [SmartCache] Liberados {freed/1024/1024:.1f} MB.")

async def save_to_disk_smart(video_id: str, data: bytes):
    """Guarda datos en disco asegurando que haya espacio."""
    if not SMART_CACHE_ENABLED:
        return
    filepath = get_cache_path(video_id)
    
    # 1. Hacer espacio antes de escribir
    # (Ejecutamos en hilo síncrono porque operaciones de disco masivas pueden bloquear)
    # Para simplificar aquí lo hacemos directo, pero idealmente async wrapper.
    _enforce_limit(len(data))
    
    # 2. Escribir
    async with aiofiles.open(filepath, mode='wb') as f:
        await f.write(data)
    
    # Actualizar fecha de acceso para que no sea el próximo en borrarse
    try:
        os.utime(filepath, None) 
    except: pass

def touch_file(video_id: str):
    """Actualiza la fecha de uso de un archivo (para que no se borre)."""
    if not SMART_CACHE_ENABLED:
        return
    filepath = get_cache_path(video_id)
    if os.path.exists(filepath):
        try:
            os.utime(filepath, None)
        except: pass