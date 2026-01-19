import sqlite3
import os
import time
import sys

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
DB_PATH = os.path.join(BASE_DIR, "database", "chats.db")

# Importamos log_timing si existe, si no usamos print simple
try:
    from utils.helpers import log_timing
except ImportError:
    def log_timing(msg): print(f"[{time.strftime('%X')}] {msg}")

def migrar_contadores_optimizado():
    if not os.path.exists(DB_PATH):
        log_timing("❌ No se encontró la base de datos.")
        return

    log_timing("🚀 Iniciando migración optimizada...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # 1. Asegurar columnas (Igual que antes)
        columns = [
            ("sin_thumb", "INTEGER DEFAULT 0"),
            ("vertical", "INTEGER DEFAULT 0"),
            ("duration_1h", "INTEGER DEFAULT 0"),
            ("blocked", "INTEGER DEFAULT 0"), 
            ("last_updated", "TEXT")
        ]
        
        cursor.execute("PRAGMA table_info(chat_video_counts)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        for col_name, col_type in columns:
            if col_name not in existing_cols:
                log_timing(f"   ➕ Agregando columna: {col_name}")
                cursor.execute(f"ALTER TABLE chat_video_counts ADD COLUMN {col_name} {col_type}")
        
        conn.commit()

        # 2. EL TRUCO: Calcular todo en memoria en UNA SOLA PASADA
        log_timing("📊 Calculando estadísticas masivas (GROUP BY)...")
        start = time.time()
        
        # Esta query se ejecuta una sola vez y agrupa todo rapidísimo
        sql_calc = """
        SELECT 
            chat_id,
            COUNT(*) as total,
            SUM(CASE WHEN has_thumb = 0 THEN 1 ELSE 0 END) as sin_thumb,
            SUM(CASE WHEN es_vertical = 1 THEN 1 ELSE 0 END) as vertical,
            SUM(CASE WHEN duracion >= 3600 THEN 1 ELSE 0 END) as duration_1h,
            SUM(CASE WHEN dump_fail = 1 AND dump_message_id IS NULL THEN 1 ELSE 0 END) as blocked
        FROM videos_telegram
        GROUP BY chat_id
        """
        cursor.execute(sql_calc)
        stats_data = cursor.fetchall() # Lista de tuplas
        
        log_timing(f"✅ Cálculo terminado en {time.time() - start:.2f}s. Procesando {len(stats_data)} chats...")

        # 3. Actualizar la tabla pequeña (Rápido)
        # Preparamos la query de actualización
        sql_update = """
        UPDATE chat_video_counts
        SET 
            videos_count = ?,
            sin_thumb = ?,
            vertical = ?,
            duration_1h = ?,
            blocked = ?,
            last_updated = CURRENT_TIMESTAMP
        WHERE chat_id = ?
        """
        
        # Reordenamos datos para que coincidan con los ? (chat_id va al final)
        batch_data = []
        for row in stats_data:
            c_id, total, no_thumb, vert, dur, block = row
            # Orden: count, sin_thumb, vertical, duration, blocked, CHAT_ID
            batch_data.append((total, no_thumb, vert, dur, block, c_id))

        # Ejecutamos en lote
        cursor.executemany(sql_update, batch_data)
        conn.commit()
        
        log_timing(f"🎉 Migración FINALIZADA en {time.time() - start:.2f}s totales.")

    except Exception as e:
        log_timing(f"❌ Error crítico: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrar_contadores_optimizado()