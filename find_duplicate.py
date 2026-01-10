import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_PATH

db_path = DB_PATH
chat_id = -1001662968475

conn = sqlite3.connect(db_path)
cur = conn.cursor()

print(f"=== Buscando duplicados en chat {chat_id} ===\n")

# Contar ocurrencias de cada video_id
cur.execute("""
    SELECT video_id, COUNT(*) as count, GROUP_CONCAT(message_id) as msg_ids
    FROM video_messages 
    WHERE chat_id = ?
    GROUP BY video_id
    ORDER BY count DESC
""", (chat_id,))

rows = cur.fetchall()

print("Videos por frecuencia:")
for video_id, count, msg_ids in rows:
    status = "🔴 DUPLICADO" if count > 1 else "✅ Único"
    print(f"  {status}: video_id={video_id[:30]}... aparece {count} veces en mensajes: {msg_ids}")

# Contar totales
total_mensajes = sum(row[1] for row in rows)
total_videos_unicos = len(rows)

print(f"\n📊 Resumen:")
print(f"  Total mensajes: {total_mensajes}")
print(f"  Videos únicos: {total_videos_unicos}")
print(f"  Duplicados: {total_mensajes - total_videos_unicos}")

conn.close()
