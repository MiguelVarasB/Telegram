import subprocess
import os

CARPETA = os.path.dirname(__file__)
dlls = ["zlib1.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll", "tdjson.dll"]

print("🔍 Analizando dependencias de DLLs con dumpbin...")
print("=" * 60)

for dll in dlls:
    ruta = os.path.join(CARPETA, dll)
    if not os.path.exists(ruta):
        print(f"\n❌ {dll} no existe")
        continue
    
    print(f"\n📦 {dll}")
    try:
        # Intentar usar dumpbin (viene con Visual Studio)
        result = subprocess.run(
            ["dumpbin", "/dependents", ruta],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            in_deps = False
            for line in lines:
                if "dependencies:" in line.lower():
                    in_deps = True
                    continue
                if in_deps and line.strip() and not line.strip().startswith("Summary"):
                    print(f"   → {line.strip()}")
                if "Summary" in line:
                    break
        else:
            print("   ⚠️ dumpbin no disponible")
    except FileNotFoundError:
        print("   ⚠️ dumpbin no encontrado (requiere Visual Studio)")
    except Exception as e:
        print(f"   ⚠️ Error: {e}")

print("\n" + "=" * 60)
print("\n💡 Si dumpbin no está disponible, instala Dependencies.exe:")
print("   https://github.com/lucasg/Dependencies/releases")
