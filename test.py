import tdjson
import os

# En la versión de diciembre 2025, el motor se encuentra aquí:
try:
    # Intentamos obtener la ruta del binario empaquetado
    dll_path = tdjson.tdjson._lib_path
    print(f"✅ Motor 2025 encontrado en: {dll_path}")
except AttributeError:
    # Si falla, simplemente imprimimos la carpeta del módulo para buscarlo manualmente
    print(f"📂 Carpeta del módulo: {os.path.dirname(tdjson.__file__)}")