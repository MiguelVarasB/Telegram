import urllib.request
import zipfile
import os
import shutil

CARPETA = os.path.dirname(__file__)

# URL del paquete NuGet específico para Windows x64
# Versión 1.8.60 (más reciente) que incluye las DLLs precompiladas
TDLIB_URL = "https://www.nuget.org/api/v2/package/tdlib.native.win-x64/1.8.60"
ZIP_FILE = os.path.join(CARPETA, "tdlib.nupkg")
EXTRACT_DIR = os.path.join(CARPETA, "tdlib_temp")

print("📥 Descargando TDLib desde GitHub (tdlib.native)...")
print(f"URL: {TDLIB_URL}")
print("=" * 60)

try:
    # Descargar
    print("\n⏬ Descargando paquete NuGet...")
    urllib.request.urlretrieve(TDLIB_URL, ZIP_FILE)
    tamaño = os.path.getsize(ZIP_FILE)
    print(f"✅ Descargado: {tamaño:,} bytes")
    
    if tamaño < 100000:
        print("⚠️ El archivo es muy pequeño, probablemente no contiene las DLLs")
        raise Exception("Archivo demasiado pequeño")
    
    # Extraer (los .nupkg son archivos ZIP)
    print("\n📦 Extrayendo paquete NuGet...")
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)
    print(f"✅ Extraído a: {EXTRACT_DIR}")
    
    # Buscar DLLs en la estructura de NuGet: runtimes/win-x64/native/
    print("\n📋 Buscando DLLs en runtimes/win-x64/native/...")
    dll_dir = os.path.join(EXTRACT_DIR, "runtimes", "win-x64", "native")
    
    if not os.path.exists(dll_dir):
        print(f"⚠️ No se encontró la carpeta: {dll_dir}")
        print("\n📂 Estructura del paquete:")
        for root, dirs, files in os.walk(EXTRACT_DIR):
            nivel = root.replace(EXTRACT_DIR, '').count(os.sep)
            indent = ' ' * 2 * nivel
            print(f'{indent}{os.path.basename(root)}/')
            sub_indent = ' ' * 2 * (nivel + 1)
            for file in files[:5]:  # Solo primeros 5 archivos
                print(f'{sub_indent}{file}')
            if len(files) > 5:
                print(f'{sub_indent}... y {len(files)-5} más')
        raise Exception("Estructura de carpetas incorrecta")
    
    # Copiar todas las DLLs encontradas
    dlls_copiadas = 0
    for file in os.listdir(dll_dir):
        if file.lower().endswith('.dll'):
            origen = os.path.join(dll_dir, file)
            destino = os.path.join(CARPETA, file)
            
            # Backup de la DLL anterior si existe
            if os.path.exists(destino):
                backup = destino + ".old"
                if os.path.exists(backup):
                    os.remove(backup)
                shutil.move(destino, backup)
                print(f"   📁 Backup: {file}.old")
            
            shutil.copy2(origen, destino)
            print(f"   ✅ Copiado: {file} ({os.path.getsize(destino):,} bytes)")
            dlls_copiadas += 1
    
    if dlls_copiadas == 0:
        raise Exception("No se encontraron DLLs para copiar")
    
    # Limpiar
    print("\n🧹 Limpiando archivos temporales...")
    os.remove(ZIP_FILE)
    shutil.rmtree(EXTRACT_DIR)
    print("✅ Limpieza completada")
    
    print("\n" + "=" * 60)
    print(f"✨ ¡Descarga completada! Se copiaron {dlls_copiadas} DLLs")
    print("\n💡 Ahora intenta ejecutar lector_tdlib.py nuevamente")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\n💡 Alternativa manual:")
    print("   1. Ve a: https://github.com/ForNeVeR/tdlib.native/releases")
    print("   2. Descarga el archivo .nupkg más reciente")
    print("   3. Renómbralo a .zip y extráelo")
    print("   4. Busca la carpeta: runtimes/win-x64/native/")
    print(f"   5. Copia todas las DLLs a: {CARPETA}")
