import sqlite3
import os
import time

# Ruta a tu base de datos
# Asume que este script está en la carpeta CLI/ y la DB en database/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "database", "chats.db")

def create_partial_indexes():
    if not os.path.exists(DB_PATH):
        print(f"❌ No se encontró la base de datos en: {DB_PATH}")
        return

    print(f"🔧 Creando índices parciales en: {os.path.basename(DB_PATH)}...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    start_time = time.time()

    try:
        # 1. Índice para videos SIN THUMB (Solo indexa donde has_thumb=0)
        print("   👉 Creando índice de 'Sin Thumb'...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stats_no_thumb ON videos_telegram(chat_id) WHERE has_thumb = 0")
        
        # 2. Índice para videos VERTICALES (Solo donde es_vertical=1)
        print("   👉 Creando índice de 'Verticales'...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stats_vertical ON videos_telegram(es_vertical) WHERE es_vertical = 1")
        
        # 3. Índice para videos LARGOS (Solo donde duracion > 1 hora)
        print("   👉 Creando índice de 'Videos Largos'...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stats_long ON videos_telegram(duracion) WHERE duracion >= 3600")

        conn.commit()
        print(f"\n✅ Índices creados exitosamente en {time.time() - start_time:.2f} segundos.")
        print("🚀 Ahora reinicia tu servidor y prueba las estadísticas.")

    except Exception as e:
        print(f"❌ Error creando índices: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_partial_indexes()