import sqlite3
import os
import time
import sys

# Ajusta la ruta si es necesario para apuntar a tu chats.db
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "chats.db")

def get_size_mb(path):
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0

def optimize_database():
    if not os.path.exists(DB_PATH):
        print(f"❌ No se encontró la base de datos en: {DB_PATH}")
        return

    print(f"📂 Base de datos: {DB_PATH}")
    size_before = get_size_mb(DB_PATH)
    print(f"📦 Peso ACTUAL: {size_before:.2f} MB ({size_before/1024:.2f} GB)")
    print("-" * 50)
    print("🚀 Iniciando optimización... (Esto puede tardar varios minutos)")
    print("   1. Activando modo WAL (Velocidad)...")
    print("   2. Ejecutando VACUUM (Compactación)...")
    print("   3. Ejecutando ANALYZE (Optimización de índices)...")
    print("-" * 50)

    start_time = time.time()

    try:
        # Conexión síncrona para mantenimiento
        conn = sqlite3.connect(DB_PATH)
        
        # 1. Activar WAL (Write-Ahead Logging) - Clave para tu problema de velocidad inicial
        conn.execute("PRAGMA journal_mode=WAL;")
        
        # 2. Sincronización NORMAL (Más rápido, seguro en discos normales)
        conn.execute("PRAGMA synchronous=NORMAL;")
        
        # 3. VACUUM: El paso pesado. Reescribe el archivo para quitar el "aire".
        #    Nota: SQLite requiere espacio libre en disco igual al tamaño de la DB para hacer esto.
        conn.execute("VACUUM;")
        
        # 4. ANALYZE: Actualiza las estadísticas para que las búsquedas sean inteligentes
        conn.execute("ANALYZE;")
        
        conn.close()
        
        end_time = time.time()
        size_after = get_size_mb(DB_PATH)
        
        print("\n✅ Optimización COMPLETADA")
        print(f"⏱️  Tiempo: {end_time - start_time:.2f} segundos")
        print(f"📦 Peso NUEVO: {size_after:.2f} MB ({size_after/1024:.2f} GB)")
        
        saved = size_before - size_after
        print(f"🎉 Espacio recuperado: {saved:.2f} MB")

    except sqlite3.OperationalError as e:
        print(f"\n❌ ERROR: {e}")
        print("   Asegúrate de que 'app.py' y ningún otro script esté usando la base de datos.")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")

if __name__ == "__main__":
    optimize_database()