"""
Procesador de archivos multimedia (Adaptado de Mega).
Ubicación: Telegram/utils/media_processor.py
"""
import subprocess
import json
import os
import asyncio
from config import (
    THUMB_WIDTH, SPRITE_COLS, SPRITE_ROWS, SPRITE_THUMB_WIDTH,
    FFMPEG_THUMB_TIMEOUT, FFMPEG_SPRITE_TIMEOUT, FFPROBE_TIMEOUT,
    THUMB_QUALITY, SPRITE_QUALITY, MIN_SPRITE_DURATION, MIN_FILE_SIZE
)

class MediaProcessor:
    """Procesador de archivos multimedia con soporte GPU"""
    
    def __init__(self):
        self.validate_dependencies()
        self._init_ffmpeg_gpu()
    
    def validate_dependencies(self):
        """Validar que FFmpeg y FFprobe estén disponibles"""
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            subprocess.run(['ffprobe', '-version'], capture_output=True, check=True)
            # print("🎬 FFmpeg y FFprobe disponibles")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ FFmpeg no encontrado. Instalar desde https://ffmpeg.org/")
            raise RuntimeError("FFmpeg requerido para procesamiento multimedia")

    def _init_ffmpeg_gpu(self):
        self.ffmpeg_gpu_flags = []
        try:
            res = subprocess.run(
                ['ffmpeg', '-hwaccel_list'],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if res.returncode == 0 and 'cuda' in (res.stdout or '').lower():
                self.ffmpeg_gpu_flags = ['-hwaccel', 'cuda']
                print("🚀 [Processor] GPU NVIDIA detectada (CUDA)")
            else:
                print("💻 [Processor] Usando CPU")
        except Exception:
            self.ffmpeg_gpu_flags = []
            print("💻 [Processor] Usando CPU (Fallback)")

    def obtener_metadatos_completos(self, url_stream):
        """Obtener metadatos (Síncrono)"""
        try:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
            cmd = [
                'ffprobe', '-v', 'error', '-select_streams', 'v:0', 
                '-show_entries', 'stream=width,height,codec_name,r_frame_rate,bit_rate:format=duration',
                '-of', 'json', '-analyzeduration', '10000000', '-probesize', '10000000', url_stream
            ]
            
            res = subprocess.run(
                cmd, capture_output=True, text=True, 
                startupinfo=startupinfo, timeout=FFPROBE_TIMEOUT
            )
            
            if res.returncode != 0:
                return {}
            
            data = json.loads(res.stdout)
            metadatos = {}
            
            if 'format' in data and 'duration' in data['format']:
                try:
                    dur = float(data['format']['duration'])
                    metadatos['duracion'] = int(dur) if dur > 0 else 0
                except:
                    metadatos['duracion'] = 0
            
            if 'streams' in data and len(data['streams']) > 0:
                stream = data['streams'][0]
                metadatos['ancho'] = stream.get('width', 0)
                metadatos['alto'] = stream.get('height', 0)
                metadatos['codec_video'] = stream.get('codec_name', '')
                
                if metadatos['ancho'] > 0 and metadatos['alto'] > 0:
                    metadatos['es_vertical'] = 1 if metadatos['alto'] > metadatos['ancho'] else 0
            
            return metadatos
        except Exception as e:
            print(f"❌ Error metadatos: {e}")
            return {}

    def generar_sprite(self, url_stream, ruta_destino, duracion):
        """Generar sprite (Síncrono)"""
        try:
            try:
                dur_int = int(duracion or 0)
            except:
                dur_int = 0

            # 1. Calcular altura proporcional 16:9 y asegurar que sea par
            # SPRITE_THUMB_WIDTH (400) * 9 / 16 = 225 -> Ajustamos a 224 (par)
            altura_raw = SPRITE_THUMB_WIDTH * 9 / 16
            altura_16_9 = int(altura_raw) // 2 * 2  

            es_corto = dur_int > 0 and dur_int <= MIN_SPRITE_DURATION

            # 2. Configurar grilla
            total_imgs = SPRITE_COLS * SPRITE_ROWS
            intervalo = max(0.1, (dur_int or 0) / total_imgs)

            # 3. Filtro FFmpeg
            filtro = (
                f"fps=1/{intervalo:.4f},"
                f"scale={SPRITE_THUMB_WIDTH}:{altura_16_9}:force_original_aspect_ratio=decrease,"
                f"pad={SPRITE_THUMB_WIDTH}:{altura_16_9}:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p,"
                f"tile={SPRITE_COLS}x{SPRITE_ROWS}"
            )

            cmd = ['ffmpeg'] + getattr(self, 'ffmpeg_gpu_flags', []) + [
                '-colorspace', 'bt709', '-color_primaries', 'bt709', '-color_trc', 'bt709',
                '-analyzeduration', '20000000', '-probesize', '20000000',
                '-i', url_stream,
                '-vf', filtro,
                '-frames:v', '1',
                '-c:v', 'libwebp', '-q:v', str(SPRITE_QUALITY), '-y', ruta_destino
            ]
            
            return self._ejecutar_ffmpeg(cmd, ruta_destino, FFMPEG_SPRITE_TIMEOUT, "SPRITE")
            
        except Exception as e:
            print(f"❌ Error generar_sprite: {e}")
            return False

    def _ejecutar_ffmpeg(self, cmd, ruta_destino, timeout, tipo):
        try:
            os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

            res = subprocess.run(
                cmd, capture_output=True, text=True, encoding='utf-8', errors='replace',
                startupinfo=startupinfo, timeout=timeout
            )

            if os.path.exists(ruta_destino) and os.path.getsize(ruta_destino) >= MIN_FILE_SIZE:
                print(f"✅ {tipo} OK: {os.path.basename(ruta_destino)}")
                return True
            
            print(f"❌ {tipo} Falló. Stderr último: {res.stderr[-300:] if res.stderr else ''}")
            return False
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout {tipo}")
            return False
        except Exception as e:
            print(f"❌ Excepción FFmpeg: {e}")
            return False

    # --- WRAPPERS ASÍNCRONOS (Para usar en FastAPI) ---
    
    async def async_obtener_metadatos(self, url_stream):
        return await asyncio.to_thread(self.obtener_metadatos_completos, url_stream)

    async def async_generar_sprite(self, url_stream, ruta_destino, duracion):
        print(f"🔄 [Async] Generando sprite para {os.path.basename(ruta_destino)}...")
        return await asyncio.to_thread(self.generar_sprite, url_stream, ruta_destino, duracion)

# Instancia global para importar
media_processor = MediaProcessor()