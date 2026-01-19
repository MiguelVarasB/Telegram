import os
from sqlcipher3 import dbapi2 as sqlite
from config import DB_PATH
from utils import log_timing
# --- CONFIGURACIÓN ---
MASTER_KEY = "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63"
DB_CIFRADA = r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\db.sqlite"
DB_PLANA = "unigram_abierta.sqlite"

def volcar_base_datos():
    if not os.path.exists(DB_CIFRADA):
        log_timing("❌ No se encuentra la base de datos original.")
        return

    if os.path.exists(DB_PLANA):
        os.remove(DB_PLANA)

    try:
        # 1. Conectar a la base cifrada
        conn = sqlite.connect(DB_CIFRADA)
        cursor = conn.cursor()
        
        # 2. Desbloquear
        cursor.execute(f"PRAGMA key = \"x'{MASTER_KEY}'\";")
        cursor.execute("PRAGMA cipher_compatibility = 4;")
        
        # 3. Exportar a una base de datos plana
        log_timing(f"🚀 Volcando datos a {DB_PLANA}...")
        cursor.execute(f"ATTACH DATABASE '{DB_PLANA}' AS plaintext KEY '';")
        cursor.execute("SELECT sqlcipher_export('plaintext');")
        cursor.execute("DETACH DATABASE plaintext;")
        
        log_timing("✅ ¡Éxito! Base de datos descifrada creada.")
        log_timing("💡 Ahora puedes abrir 'unigram_abierta.sqlite' con DB Browser for SQLite.")

    except Exception as e:
        log_timing(f"❌ Error durante el volcado: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    volcar_base_datos()