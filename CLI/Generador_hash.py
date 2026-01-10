import os
import subprocess
import json
import hashlib
import time

class VideoHasherTool:
    def __init__(self):
        # Extensiones de video comunes
        self.video_extensions = ('.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.webm')

    def get_video_metadata(self, file_path):
        """
        Extrae metadatos precisos usando ffprobe.
        """
        cmd = [
            'ffprobe', 
            '-v', 'error', 
            '-select_streams', 'v:0', 
            '-show_entries', 'stream=width,height,duration,nb_frames', 
            '-of', 'json', 
            file_path
        ]
        
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            data = json.loads(result.stdout)
            
            if 'streams' in data and len(data['streams']) > 0:
                stream = data['streams'][0]
                return {
                    'width': stream.get('width', 0),
                    'height': stream.get('height', 0),
                    'duration': stream.get('duration', '0'),
                    'nb_frames': stream.get('nb_frames', '0'),
                    'size': os.path.getsize(file_path)
                }
        except Exception as e:
            print(f"Error al obtener metadatos de {file_path}: {e}")
        return None

    def generate_unique_hash(self, meta):
        """
        Crea un hash de 12 caracteres basado en la huella digital del archivo.
        """
        # Creamos una cadena única con los metadatos técnicos
        # Incluimos tamaño, duración, resolución y frames
        fingerprint = f"{meta['size']}_{meta['duration']}_{meta['width']}x{meta['height']}_{meta['nb_frames']}"
        
        # Generamos SHA256 y recortamos a 12 caracteres
        full_hash = hashlib.sha256(fingerprint.encode()).hexdigest()
        return full_hash[:12]

    def process_directory(self, root_path):
        """
        Recorre la ruta y genera los hashes midiendo el tiempo.
        """
        if not os.path.isdir(root_path):
            print(f"La ruta no es válida: {root_path}")
            return

        print(f"{'Archivo':<50} | {'Hash':<15} | {'Tiempo':<10}")
        print("-" * 80)

        for root, dirs, files in os.walk(root_path):
            for file in files:
                if file.lower().endswith(self.video_extensions):
                    full_path = os.path.join(root, file)
                    
                    # Medir tiempo de inicio
                    start_time = time.time()
                    
                    # 1. Obtener Metadatos
                    meta = self.get_video_metadata(full_path)
                    
                    if meta:
                        # 2. Generar Hash
                        v_hash = self.generate_unique_hash(meta)
                        
                        # Calcular tiempo transcurrido
                        elapsed_time = time.time() - start_time
                        
                        # Mostrar resultado
                        short_name = (file[:47] + '..') if len(file) > 50 else file
                        print(f"{short_name:<50} | {v_hash:<15} | {elapsed_time:.4f}s")
                    else:
                        print(f"No se pudo procesar: {file}")

if __name__ == "__main__":
    # CONFIGURACIÓN: Ingresa tu ruta aquí
    ruta_a_indexar = input("Introduce la ruta de la carpeta de videos: ").strip()
    
    # Limpiar comillas si el usuario arrastra la carpeta a la consola
    ruta_a_indexar = ruta_a_indexar.replace('"', '').replace("'", "")
    
    hasher = VideoHasherTool()
    
    print("\nIniciando generación de hashes...\n")
    hasher.process_directory(ruta_a_indexar)
    print("\nProceso finalizado.")