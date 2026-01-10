import os
import time
import torch
from faster_whisper import WhisperModel

def analizar_video_detallado():
    # 1. Ruta del archivo
    ruta_video = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\videos\4947552057247662568.mp4"

    if not os.path.exists(ruta_video):
        print(f"Error: No se encontró el archivo.")
        return

    # 2. Carga del modelo
    print("Iniciando IA en RTX 3050...")
    # Cargamos el modelo una vez
    model = WhisperModel("base", device="cuda", compute_type="float16")

    print(f"--- Iniciando análisis técnico de archivo pesado ---")
    
    # 3. Medición de tiempo de IA
    inicio_ia = time.perf_counter()

    # Whisper analiza el flujo de audio
    segments, info = model.transcribe(ruta_video, beam_size=1)
    
    # Obtenemos el primer segmento para confirmar que se procesó audio
    primer_segmento = next(segments, None)
    
    fin_ia = time.perf_counter()

    # 4. Recolección de datos técnicos
    tamaño_bytes = os.path.getsize(ruta_video)
    tamaño_mb = tamaño_bytes / (1024 * 1024)
    duracion_segundos = info.duration
    tiempo_deteccion = fin_ia - inicio_ia
    
    # Evitar división por cero si el video es corrupto
    bitrate = (tamaño_mb * 8) / duracion_segundos if duracion_segundos > 0 else 0

    # 5. Reporte de Análisis (Corregido)
    print("\n" + "="*45)
    print(f"📊 REPORTE DE RENDIMIENTO (ARCHIVO PESADO)")
    print("="*45)
    print(f"Archivo:      {os.path.basename(ruta_video)}")
    print(f"Tamaño:       {tamaño_mb:.2f} MB")
    print(f"Duración:     {duracion_segundos:.2f} segundos ({duracion_segundos/60:.2f} min)")
    print(f"Bitrate est.: {bitrate:.2f} Mbps")
    print("-" * 45)
    print(f"Idioma:       {info.language.upper()} (Confianza: {info.language_probability:.2%})")
    print(f"⏱️ TIEMPO IA:  {tiempo_deteccion:.4f} segundos")
    print(f"🚀 VELOCIDAD: {duracion_segundos / tiempo_deteccion:.2f}x (Tiempo real vs IA)")
    print("="*45)

    if primer_segmento:
        print(f"\nPrimer diálogo detectado: \"{primer_segmento.text}\"")
    else:
        print("\nNo se detectó diálogo en los primeros segundos.")

if __name__ == "__main__":
    analizar_video_detallado()