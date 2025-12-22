"""
Script de migración para agregar soporte de persistencia (Dump Channel).
"""
import sqlite3
import os

# Ajusta la ruta si es necesario
DB_PATH = os.path.join("database", "chats.db")
NOMBRE_COLUMNA="dump_fail"
COLUMNA_TIPO="INTEGER"
def migrar_db():
    print(f"🛠️  Abriendo base de datos: {DB_PATH}...")
    if not os.path.exists(DB_PATH):
        print("❌ Error: No encuentro el archivo de base de datos.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Añadimos la columna 'dump_fail' para marcar errores al enviar al canal Dump.
        cursor.execute(f"ALTER TABLE videos_telegram ADD COLUMN {NOMBRE_COLUMNA} {COLUMNA_TIPO} DEFAULT NULL")
        print(f"✅ Columna '{NOMBRE_COLUMNA}' agregada con éxito.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("ℹ️  La columna ya existía. Todo está bien.")
        else:
            print(f"❌ Error SQL: {e}")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrar_db()