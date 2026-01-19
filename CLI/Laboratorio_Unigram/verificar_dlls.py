import ctypes
import os
import sys

CARPETA = os.path.dirname(__file__)
dlls = ["zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll", "tdjson.dll"]

print("🔍 Verificando DLLs en:", CARPETA)
print("=" * 60)

for dll in dlls:
    ruta = os.path.join(CARPETA, dll)
    existe = os.path.exists(ruta)
    print(f"\n📦 {dll}")
    print(f"   Existe: {'✅' if existe else '❌'}")
    
    if existe:
        try:
            lib = ctypes.CDLL(ruta)
            print(f"   Carga:  ✅ OK")
        except Exception as e:
            print(f"   Carga:  ❌ FALLO")
            print(f"   Error:  {str(e)[:80]}")

print("\n" + "=" * 60)
print("\n💡 Si todas fallan al cargar, necesitas instalar:")
print("   Visual C++ Redistributable 2015-2022 (x64)")
print("   https://aka.ms/vs/17/release/vc_redist.x64.exe")
