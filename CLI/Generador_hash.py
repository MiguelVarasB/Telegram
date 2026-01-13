import os
import time

from utils.video_hasher import VIDEO_EXTENSIONS, hash_video_file

class VideoHasherTool:
    def __init__(self):
        # Extensiones de video comunes (reusadas desde utils)
        self.video_extensions = VIDEO_EXTENSIONS

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
                    
                    v_hash = hash_video_file(full_path)

                    # Calcular tiempo transcurrido
                    elapsed_time = time.time() - start_time

                    # Mostrar resultado
                    short_name = (file[:47] + '..') if len(file) > 50 else file
                    if v_hash:
                        print(f"{short_name:<50} | {v_hash:<15} | {elapsed_time:.4f}s")
                    else:
                        print(f"No se pudo procesar: {short_name}")

if __name__ == "__main__":
    # CONFIGURACIÓN: Ingresa tu ruta aquí
    ruta_a_indexar = input("Introduce la ruta de la carpeta de videos: ").strip()
    
    # Limpiar comillas si el usuario arrastra la carpeta a la consola
    ruta_a_indexar = ruta_a_indexar.replace('"', '').replace("'", "")
    
    hasher = VideoHasherTool()
    
    print("\nIniciando generación de hashes...\n")
    hasher.process_directory(ruta_a_indexar)
    print("\nProceso finalizado.")