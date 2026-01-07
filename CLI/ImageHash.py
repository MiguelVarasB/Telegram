import os
import imagehash
from PIL import Image
from pathlib import Path

def calcular_distancia_hex(h1, h2):
    return imagehash.hex_to_hash(h1) - imagehash.hex_to_hash(h2)

def analizador_de_similitudes_total(ruta_carpeta, umbral_similitud=12):
    extensiones = {'.jpg', '.jpeg', '.png', '.webp'}
    archivos = [p for p in Path(ruta_carpeta).glob('*') if p.suffix.lower() in extensiones]
    
    # 1. Generar todos los hashes primero
    catalogo = []
    print(f"[*] Generando hashes para {len(archivos)} archivos...")
    
    for arc in archivos:
        try:
            with Image.open(arc) as img:
                h = str(imagehash.phash(img))
                catalogo.append({'nombre': arc.name, 'hash': h, 'ruta': str(arc)})
        except:
            continue

    print("\n" + "="*80)
    print(f"{'ANÁLISIS DE CERCANÍA (Umbral: ' + str(umbral_similitud) + ')':^80}")
    print("="*80)

    # 2. Comparar cada imagen contra las demás (Matriz de similitud)
    ya_comparados = set()
    encontrados = False

    for i in range(len(catalogo)):
        for j in range(i + 1, len(catalogo)):
            img1 = catalogo[i]
            img2 = catalogo[j]
            
            distancia = calcular_distancia_hex(img1['hash'], img2['hash'])
            
            if distancia <= umbral_similitud:
                encontrados = True
                status = "COINCIDENCIA EXACTA" if distancia == 0 else "MUY SIMILAR"
                print(f"[{status}] Distancia: {distancia}")
                print(f"  A: {img1['nombre']} ({img1['hash']})")
                print(f"  B: {img2['nombre']} ({img2['hash']})")
                print("-" * 80)

    if not encontrados:
        print("No se encontraron imágenes similares con el umbral actual.")

if __name__ == "__main__":
    carpeta_probar = r"C:\Users\TheMiguel\Downloads\JDownloader\Probar"
    # Bajamos el umbral a 12 para ser más permisivos con los formatos y bordes
    analizador_de_similitudes_total(carpeta_probar, umbral_similitud=15)