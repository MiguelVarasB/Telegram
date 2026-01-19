import urllib.request
import zipfile
import os
import shutil

CARPETA = os.path.dirname(__file__)

# URL de TDLib precompilado para Windows x64 desde NuGet
# Usamos la versión 1.8.45 del paquete tdlib.native
TDLIB_URL = "https://www.nuget.org/api/v2/package/tdlib.native/1.8.45"
ZIP_FILE = os.path.join(CARPETA, "tdlib.zip")
EXTRACT_DIR = os.path.join(CARPETA, "tdlib_temp")

print("📥 Descargando TDLib oficial para Windows x64...")
print(f"URL: {TDLIB_URL}")
print("=" * 60)

try:
    # Descargar
    print("\n⏬ Descargando archivo ZIP...")
    urllib.request.urlretrieve(TDLIB_URL, ZIP_FILE)
    print(f"✅ Descargado: {os.path.getsize(ZIP_FILE):,} bytes")
    
    # Extraer
    print("\n📦 Extrayendo archivos...")
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)
    print(f"✅ Extraído a: {EXTRACT_DIR}")
    
    # Buscar y copiar DLLs
    print("\n📋 Buscando DLLs necesarias...")
    dlls_necesarias = ["tdjson.dll", "zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll"]
    
    for root, dirs, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.lower() in [d.lower() for d in dlls_necesarias]:
                origen = os.path.join(root, file)
                destino = os.path.join(CARPETA, file)
                
                # Backup de la DLL anterior si existe
                if os.path.exists(destino):
                    backup = destino + ".old"
                    shutil.move(destino, backup)
                    print(f"   📁 Backup: {file}.old")
                
                shutil.copy2(origen, destino)
                print(f"   ✅ Copiado: {file} ({os.path.getsize(destino):,} bytes)")
    
    # Limpiar
    print("\n🧹 Limpiando archivos temporales...")
    os.remove(ZIP_FILE)
    shutil.rmtree(EXTRACT_DIR)
    print("✅ Limpieza completada")
    
    print("\n" + "=" * 60)
    print("✨ ¡Descarga completada!")
    print("\n💡 Ahora intenta ejecutar lector_tdlib.py nuevamente")
    
except Exception as e:
    print(f"\n❌ Error durante la descarga: {e}")
    print("\n💡 Alternativa manual:")
    print(f"   1. Descarga: {TDLIB_URL}")
    print(f"   2. Extrae el ZIP")
    print(f"   3. Copia las DLLs a: {CARPETA}")
