import os
import re
import binascii
from Registry import Registry

RUTA_BASE = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw"

def hex_dump(data, length=256):
    """Crea una vista estilo editor hexadecimal (Offset | Hex | ASCII)."""
    result = []
    for i in range(0, min(len(data), length), 16):
        chunk = data[i:i+16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        ascii_part = "".join(chr(b) if 32 <= b <= 126 else "." for b in chunk)
        result.append(f"{i:08x} | {hex_part:<47} | {ascii_part}")
    return "\n".join(result)

def volcar_registro(ruta):
    """Muestra TODAS las llaves y valores de un archivo de registro."""
    try:
        reg = Registry.Registry(ruta)
        print("    ✅ Estructura de Registro Detectada:")
        
        def recorrer_todo(key, indent=6):
            # Mostrar la llave actual
            print(f"{' ' * indent}📂 Llave: {key.name()}")
            
            # Mostrar todos los valores de esta llave
            for val in key.values():
                try:
                    print(f"{' ' * (indent + 4)}🔹 {val.name()}: {val.value()}")
                except:
                    print(f"{' ' * (indent + 4)}🔹 {val.name()}: [Error al leer valor]")
            
            # Recurrir en subllaves
            for subkey in key.subkeys():
                recorrer_todo(subkey, indent + 4)
        
        recorrer_todo(reg.root())
        return True
    except Exception:
        return False

def volcar_binario(ruta):
    """Muestra el volcado hexadecimal y strings de un archivo binario."""
    try:
        with open(ruta, "rb") as f:
            contenido = f.read()
            
            if not contenido:
                print("    [Archivo vacío]")
                return

            print("    📊 Volcado Hexadecimal (Primeros 512 bytes):")
            print(hex_dump(contenido, 512))
            
            print("\n    📖 Cadenas de texto encontradas (Strings):")
            # Extrae todas las secuencias de texto legible de 4 o más caracteres
            strings = re.findall(b"[\x20-\x7E]{4,}", contenido)
            for s in strings[:30]: # Limitamos a 30 para no saturar la terminal
                try:
                    print(f"      \"{s.decode('ascii')}\"")
                except:
                    continue
            if len(strings) > 30:
                print(f"      ... (y {len(strings)-30} cadenas más)")
                
    except Exception as e:
        print(f"    ❌ Error al abrir: {e}")

def ejecutar_volcado_total():
    print(f"🚀 INICIANDO VOLCADO TOTAL DE DATOS EN: {RUTA_BASE}")
    print("="*100)
    
    for raiz, carpetas, archivos in os.walk(RUTA_BASE):
        for nombre_archivo in archivos:
            if nombre_archivo.lower().endswith(".dat"):
                ruta_completa = os.path.join(raiz, nombre_archivo)
                rel_path = os.path.relpath(ruta_completa, RUTA_BASE)
                
                print(f"\n📄 ARCHIVO: {rel_path}")
                print("-" * (len(rel_path) + 12))
                
                # 1. Intentar mostrar como Registro (estructurado)
                es_registro = volcar_registro(ruta_completa)
                
                # 2. Si no es registro o después de mostrar el registro, mostrar binario
                if not es_registro:
                    print("    📦 Formato No Estructurado / Binario:")
                    volcar_binario(ruta_completa)
                
                print("\n" + "."*80)

if __name__ == "__main__":
    ejecutar_volcado_total()