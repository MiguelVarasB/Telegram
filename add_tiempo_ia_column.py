import sqlite3
from pathlib import Path

# Obtener ruta a la base de datos
BASE_DIR = Path(__file__).resolve().parents[0]
BD_UNIGRAM = BASE_DIR / "database/unigram.db"

conn = sqlite3.connect(BD_UNIGRAM)
cursor = conn.cursor()

try:
    # Agregar la columna modelo a la tabla cacheo
    cursor.execute("ALTER TABLE cacheo ADD COLUMN modelo TEXT")
    print("✅ Columna 'modelo' agregada exitosamente a la tabla 'cacheo'")
    
    # Verificar la nueva estructura
    cursor.execute('PRAGMA table_info(cacheo)')
    print("\nEstructura actualizada de la tabla 'cacheo':")
    for row in cursor.fetchall():
        print(f"  {row}")
    
    conn.commit()
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("⚠️ La columna 'modelo' ya existe en la tabla 'cacheo'")
    else:
        print(f"❌ Error: {e}")
finally:
    conn.close()
