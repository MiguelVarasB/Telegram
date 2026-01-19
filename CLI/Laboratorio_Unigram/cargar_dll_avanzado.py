import ctypes
import ctypes.wintypes
import os
import sys

CARPETA = os.path.dirname(__file__)

# Constantes de Windows para LoadLibraryEx
LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008
LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100
LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000

# Cargar kernel32
kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
LoadLibraryExW = kernel32.LoadLibraryExW
LoadLibraryExW.argtypes = [ctypes.wintypes.LPCWSTR, ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD]
LoadLibraryExW.restype = ctypes.wintypes.HMODULE

GetLastError = kernel32.GetLastError
GetLastError.argtypes = []
GetLastError.restype = ctypes.wintypes.DWORD

# Agregar directorio al DLL search path
if hasattr(os, 'add_dll_directory'):
    os.add_dll_directory(CARPETA)
    print(f"✅ Directorio agregado al DLL search path: {CARPETA}")

dlls = ["zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll", "tdjson.dll"]

print("\n🔍 Intentando cargar DLLs con LoadLibraryEx...")
print("=" * 60)

for dll in dlls:
    ruta = os.path.join(CARPETA, dll)
    if not os.path.exists(ruta):
        print(f"\n❌ {dll} no existe")
        continue
    
    print(f"\n📦 {dll}")
    print(f"   Ruta: {ruta}")
    print(f"   Tamaño: {os.path.getsize(ruta):,} bytes")
    
    # Intentar diferentes métodos de carga
    metodos = [
        ("LOAD_WITH_ALTERED_SEARCH_PATH", LOAD_WITH_ALTERED_SEARCH_PATH),
        ("LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | SEARCH_DEFAULT_DIRS", 
         LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS),
        ("LOAD_LIBRARY_SEARCH_DEFAULT_DIRS", LOAD_LIBRARY_SEARCH_DEFAULT_DIRS),
    ]
    
    cargado = False
    for nombre_metodo, flag in metodos:
        try:
            handle = LoadLibraryExW(ruta, None, flag)
            if handle:
                print(f"   ✅ Cargado con {nombre_metodo}")
                cargado = True
                break
            else:
                error = GetLastError()
                print(f"   ⚠️ {nombre_metodo}: Error {error}")
        except Exception as e:
            print(f"   ⚠️ {nombre_metodo}: {str(e)[:50]}")
    
    if not cargado:
        print(f"   ❌ No se pudo cargar con ningún método")

print("\n" + "=" * 60)
print("\n💡 Si ninguna DLL se carga, las posibles causas son:")
print("   1. Las DLLs requieren dependencias que no están en el sistema")
print("   2. Las DLLs son para una arquitectura diferente (ARM vs x64)")
print("   3. Las DLLs están corruptas o incompletas")
print("\n📥 Descarga Dependencies.exe para análisis detallado:")
print("   https://github.com/lucasg/Dependencies/releases")
