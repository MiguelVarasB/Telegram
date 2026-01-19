"""
CONFIGURACIÓN OPTIMIZADA PARA HARDWARE DE ALTO RENDIMIENTO
=========================================================

Este archivo contiene los parámetros ajustables para la versión optimizada
del indexador de archivos de Telegram.

PARÁMETROS DE RENDIMIENTO:
--------------------------
- MAX_WORKERS: Número máximo de procesos paralelos (recomendado: CPU cores - 1)
- BATCH_SIZE: Tamaño de lotes para procesamiento (mayor = más RAM, mejor throughput)
- DB_COMMIT_SIZE: Frecuencia de commits a BD (mayor = menos I/O, más riesgo)
- CHUNK_SIZE: Tamaño de chunks para workers (balance entre carga y memoria)

AJUSTES RECOMENDADOS SEGÚN HARDWARE:
------------------------------------
- 15 núcleos, 64GB RAM: MAX_WORKERS=12, BATCH_SIZE=1000, CHUNK_SIZE=2000
- 8 núcleos, 32GB RAM: MAX_WORKERS=6, BATCH_SIZE=500, CHUNK_SIZE=1000
- 4 núcleos, 16GB RAM: MAX_WORKERS=3, BATCH_SIZE=250, CHUNK_SIZE=500
"""

import multiprocessing as mp
import psutil

# Detección automática de hardware
CPU_CORES = mp.cpu_count()
AVAILABLE_RAM_GB = psutil.virtual_memory().total / (1024**3)

# Configuración base según hardware detectado
if CPU_CORES >= 12 and AVAILABLE_RAM_GB >= 48:
    # Hardware de alto rendimiento (Tu Xeon + 64GB)
    MAX_WORKERS = CPU_CORES - 1  # Usa 15 núcleos
    BATCH_SIZE = 1000
    DB_COMMIT_SIZE = 5000        # Commits más espaciados para ganar velocidad
    CHUNK_SIZE = 200             # Bloques más pequeños para saturar todos los núcleos
    ENABLE_AGGRESSIVE_OPTIMIZATION = True
    
elif CPU_CORES >= 8 and AVAILABLE_RAM_GB >= 24:
    # Hardware medio-alto
    MAX_WORKERS = min(6, CPU_CORES - 1)
    BATCH_SIZE = 500
    DB_COMMIT_SIZE = 1000
    CHUNK_SIZE = 1000
    ENABLE_AGGRESSIVE_OPTIMIZATION = False
    
else:
    # Hardware estándar
    MAX_WORKERS = min(3, CPU_CORES - 1)
    BATCH_SIZE = 250
    DB_COMMIT_SIZE = 500
    CHUNK_SIZE = 500
    ENABLE_AGGRESSIVE_OPTIMIZATION = False

# Configuración de pipeline
Tiempo_entre_escaneos = 15 if ENABLE_AGGRESSIVE_OPTIMIZATION else 30
NUM_CICLOS = 3 if ENABLE_AGGRESSIVE_OPTIMIZATION else 5

# Configuración de base de datos
DB_TIMEOUT = 30.0  # segundos
DB_RETRY_ATTEMPTS = 3

# Configuración de logging
LOG_BATCH_SIZE = 100  # Mostrar progreso cada N archivos
ENABLE_DETAILED_LOGGING = False  # False para máximo rendimiento

def print_config():
    """Muestra la configuración actual de optimización."""
    print("⚙️ CONFIGURACIÓN DE OPTIMIZACIÓN DETECTADA:")
    print(f"   🖥️  CPU: {CPU_CORES} núcleos")
    print(f"   💾 RAM: {AVAILABLE_RAM_GB:.1f} GB")
    print(f"   ⚡ Workers: {MAX_WORKERS}")
    print(f"   📦 Batch size: {BATCH_SIZE}")
    print(f"   💾 DB commit size: {DB_COMMIT_SIZE}")
    print(f"   🧩 Chunk size: {CHUNK_SIZE}")
    print(f"   🔄 Ciclos pipeline: {NUM_CICLOS}")
    print(f"   ⏱️  Espera entre ciclos: {Tiempo_entre_escaneos}s")
    print(f"   🚀 Optimización agresiva: {'SÍ' if ENABLE_AGGRESSIVE_OPTIMIZATION else 'NO'}")

if __name__ == "__main__":
    print_config()
