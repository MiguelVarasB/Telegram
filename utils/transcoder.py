import os
import subprocess
import asyncio

def compress_video_nvenc(input_path: str, output_path: str, crf: int = 28):
    """
    Comprime un video usando la GPU NVIDIA (NVENC) a H.265 (HEVC).
    
    Args:
        input_path: Ruta del video original.
        output_path: Ruta donde guardar el comprimido.
        crf: Factor de calidad Constant Rate (0-51). 
             28 es un buen balance peso/calidad para H.265.
             Menor número = Más calidad/Más peso.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No existe el archivo: {input_path}")

    # Comando FFmpeg optimizado para RTX 3050
    command = [
        'ffmpeg', 
        '-y',                 # Sobrescribir si existe
        '-hwaccel', 'cuda',   # Usar aceleración CUDA para decodificar
        '-i', input_path,     # Archivo de entrada
        '-c:v', 'hevc_nvenc', # CODIFICADOR DE VIDEO: H.265 por GPU
        '-preset', 'p4',      # Preset de velocidad/calidad (p1=rápido, p7=lento). p4 es medio.
        '-rc', 'vbr',         # Variable Bitrate
        '-cq', str(crf),      # Calidad constante (controla el peso)
        '-b:v', '0',          # Dejar que el CQ controle el bitrate
        '-c:a', 'copy',       # Copiar el audio tal cual (no re-comprimir audio para no perder calidad ahí)
        output_path
    ]

    print(f"🚀 [GPU] Iniciando compresión NVENC: {os.path.basename(input_path)}")
    
    # Ejecutamos FFmpeg
    # hide_banner y loglevel error para no ensuciar la consola
    process = subprocess.run(
        command, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE
    )

    if process.returncode != 0:
        error_msg = process.stderr.decode('utf-8', errors='ignore')
        raise RuntimeError(f"FFmpeg falló: {error_msg}")
        
    print(f"✅ [GPU] Compresión terminada: {os.path.basename(output_path)}")
    return True

async def async_compress_video(input_path: str, output_path: str):
    """Wrapper asíncrono para no bloquear el servidor."""
    return await asyncio.to_thread(compress_video_nvenc, input_path, output_path)