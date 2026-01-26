import subprocess
import os
import math
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURACIÓN PARA EL SISTEMA DE THEMIGUEL ---
VIDEO_PATH = r"C:\Users\TheMiguel\Downloads\JDownloader\Probar\VideoDePrueba.mp4"
OUTPUT_DIR = r"C:\Users\TheMiguel\Downloads\JDownloader\Probar\Sprites"
INTERVALO_SEGUNDOS = 60
GRID_LADO = 4
FRAME_W = 448
FRAME_H = 256

def log_timing(msg: str):
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {msg}")

def get_duration(file_path):
    cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return float(result.stdout)

def procesar_segmento(args):
    """Ejecuta FFmpeg para un solo sprite con máxima aceleración GPU."""
    inicio, duracion_seg, output_name, idx = args
    
    # FILTRO VELOZ: 
    # 1. fps: Salta de forma eficiente.
    # 2. scale: Ajusta el tamaño antes del tile.
    # 3. tile: Une los cuadros sin márgenes.
    filtro = f"fps=1/{INTERVALO_SEGUNDOS},scale={FRAME_W}:{FRAME_H},tile={GRID_LADO}x{GRID_LADO}"

    cmd = [
        'ffmpeg', '-y',
        '-hwaccel', 'cuda',              # Decodificación en la 3050
        '-ss', str(inicio),              # Salto rápido ANTES de -i
        '-i', VIDEO_PATH,
        '-t', str(duracion_seg),         # Solo el pedazo necesario
        '-vf', filtro,
        '-frames:v', '1',
        '-qscale:v', '3',                # Calidad balanceada para MJPEG
        '-threads', '0',                 # Usa todos los cores disponibles
        output_name
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return f"[+] Parte {idx} finalizada."
    except Exception as e:
        return f"[!] Error en Parte {idx}: {e}"

def generar_mega_sprites_limpios():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    duracion_total = get_duration(VIDEO_PATH)
    frames_per_sprite = GRID_LADO * GRID_LADO
    tiempo_per_sprite = frames_per_sprite * INTERVALO_SEGUNDOS
    total_sprites = math.ceil(duracion_total / tiempo_per_sprite)

    log_timing(f"[*] Iniciando PARALELISMO. Total: {total_sprites} sprites.")

    tareas = []
    for i in range(total_sprites):
        inicio = i * tiempo_per_sprite
        nombre_salida = os.path.join(OUTPUT_DIR, f"sprite_limpio_{i+1}.jpg")
        tareas.append((inicio, tiempo_per_sprite, nombre_salida, i+1))

    # Aprovechamos tus 32 hilos del Xeon lanzando 6 procesos a la vez
    # Esto satura mejor el NVDEC de tu RTX 3050 sin sobrecargarlo
    with ProcessPoolExecutor(max_workers=6) as executor:
        resultados = list(executor.map(procesar_segmento, tareas))

    for r in resultados:
        print(r)

    log_timing(f"[LISTO] Sprites generados en tiempo récord.")

if __name__ == "__main__":
    generar_mega_sprites_limpios()