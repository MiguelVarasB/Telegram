import subprocess
import os
import json

def obtener_metadata_audio(ruta_video):
    """Detecta el bitrate y sample rate original del video usando ffprobe."""
    comando = [
        'ffprobe', '-v', 'error', '-select_streams', 'a:0',
        '-show_entries', 'stream=bit_rate,sample_rate',
        '-of', 'json', ruta_video
    ]
    try:
        resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
        datos = json.loads(resultado.stdout)
        stream = datos['streams'][0]
        
        # Bitrate en bps (ej: 96400) y Sample Rate (ej: 48000)
        bitrate = stream.get('bit_rate', '128k') # Fallback a 128k si no se detecta
        sample_rate = stream.get('sample_rate', '44100')
        return bitrate, sample_rate
    except Exception as e:
        print(f"No se pudo leer la metadata: {e}. Usando valores por defecto.")
        return '128k', '44100'

def extraer_audio_a_weba(ruta_video):
    """Extrae audio a WebA replicando la calidad original del video."""
    bitrate, sample_rate = obtener_metadata_audio(ruta_video)
    ruta_salida = os.path.splitext(ruta_video)[0] + ".weba"
    
    # Usamos los valores detectados dinámicamente
    comando = [
        'ffmpeg', '-i', ruta_video,
        '-vn',
        '-c:a', 'libopus',
        '-b:a', bitrate,
        '-ar', sample_rate,
        '-f', 'webm',
        '-y', ruta_salida
    ]
    
    print(f"--- Paso 1: Extrayendo a WebA ({bitrate} bps, {sample_rate} Hz) ---")
    subprocess.run(comando, check=True)
    return ruta_salida

def convertir_weba_a_flac(ruta_weba):
    """Convierte a FLAC optimizado para IA (16kHz, Mono)."""
    ruta_salida = os.path.splitext(ruta_weba)[0] + "_listo_IA.flac"
    
    # Configuración estándar para máxima precisión en Nova 3 / Whisper
    comando = [
        'ffmpeg', '-i', ruta_weba,
        '-ac', '1',             # Mono (reduce 50%)
        '-ar', '16000',         # 16 kHz (estándar IA)
        '-sample_fmt', 's16',   # <--- AHORRO CRÍTICO (16 bits)
        '-y', ruta_salida
    ]
    
    print(f"--- Paso 2: Optimizando FLAC para IA (16kHz, Mono) ---")
    subprocess.run(comando, check=True)
    return ruta_salida

# --- EJECUCIÓN ---
ruta = r"C:\Users\TheMiguel\Downloads\JDownloader\VideoDePrueba.mp4"

if os.path.exists(ruta):
    archivo_weba = extraer_audio_a_weba(ruta)
    archivo_flac = convertir_weba_a_flac(archivo_weba)
    print(f"\nProceso finalizado. El WebA ahora respeta el original.")