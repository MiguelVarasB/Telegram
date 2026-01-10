import os
import json
import re

# --- CONFIGURACIÓN ---
RUTA_CACHE = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\videos"
CantidadVideos = 1000

def listar_videos_para_muestra():
    """
    Lista todos los videos que no tengan un número como nombre y los exporta a JSON
    """
    videos_sin_numero = []
    
    if not os.path.exists(RUTA_CACHE):
        print(f"Error: No existe la ruta {RUTA_CACHE}")
        return
    
    # Obtener todos los archivos .mp4 en la carpeta
    archivos = [f for f in os.listdir(RUTA_CACHE) if f.endswith('.mp4')]
    
    print(f"Total de videos encontrados: {len(archivos)}")
    
    # Filtrar videos que no tengan números en el nombre
    for archivo in archivos:
        # Eliminar extensión para verificar el nombre
        nombre_sin_ext = os.path.splitext(archivo)[0]
        
        # Verificar si el nombre contiene algún número
        if not re.search(r'\d', nombre_sin_ext):
            ruta_completa = os.path.join(RUTA_CACHE, archivo)
            info_video = {
                "nombre": nombre_sin_ext,
                "archivo": archivo,
                "ruta": ruta_completa,
                "tamano_bytes": os.path.getsize(ruta_completa) if os.path.exists(ruta_completa) else 0
            }
            videos_sin_numero.append(info_video)
    
    print(f"Videos sin número en el nombre: {len(videos_sin_numero)}")
    
    # Limitar a la cantidad solicitada si hay más
    if len(videos_sin_numero) > CantidadVideos:
        videos_sin_numero = videos_sin_numero[:CantidadVideos]
        print(f"Limitando a los primeros {CantidadVideos} videos")
    
    # Exportar a JSON
    nombre_salida = "videos_sin_numero.json"
    with open(nombre_salida, 'w', encoding='utf-8') as f:
        json.dump(videos_sin_numero, f, ensure_ascii=False, indent=2)
    
    print(f"Exportados {len(videos_sin_numero)} videos a {nombre_salida}")
    
    # Mostrar lista de videos encontrados
    print("\nVideos encontrados:")
    for i, video in enumerate(videos_sin_numero, 1):
        print(f"{i}. {video['nombre']}")
    
    return videos_sin_numero

if __name__ == "__main__":
    listar_videos_para_muestra()
