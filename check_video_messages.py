import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_PATH

db_path = DB_PATH
chat_id = -1001662968475

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Verificar tablas
print("=== Tablas en la BD ===")
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cur.fetchall()]
for t in tables:
    print(f"  - {t}")

# Verificar video_messages
if "video_messages" in tables:
    print(f"\n=== Mensajes en video_messages para chat {chat_id} ===")
    cur.execute("SELECT message_id, video_id FROM video_messages WHERE chat_id = ? ORDER BY message_id", (chat_id,))
    rows = cur.fetchall()
    print(f"Total: {len(rows)} mensajes")
    for msg_id, video_id in rows:
        print(f"  msg_id={msg_id}, video_id={video_id[:30]}...")
else:
    print("\n⚠️ Tabla video_messages NO existe")

# Verificar videos_telegram
if "videos_telegram" in tables:
    print(f"\n=== Videos únicos en videos_telegram para chat {chat_id} ===")
    cur.execute("SELECT file_unique_id, nombre FROM videos_telegram WHERE chat_id = ? ORDER BY file_unique_id", (chat_id,))
    rows = cur.fetchall()
    print(f"Total: {len(rows)} videos únicos")
    for file_id, name in rows:
        print(f"  file_unique_id={file_id[:30]}..., name={name}")
else:
    print("\n⚠️ Tabla videos_telegram NO existe")

# Verificar chat_video_counts
if "chat_video_counts" in tables:
    print(f"\n=== Contadores en chat_video_counts para chat {chat_id} ===")
    cur.execute("SELECT videos_count, indexados, duplicados FROM chat_video_counts WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    if row:
        print(f"  videos_count={row[0]}, indexados={row[1]}, duplicados={row[2]}")
    else:
        print(f"  ⚠️ No hay registro para chat {chat_id}")
else:
    print("\n⚠️ Tabla chat_video_counts NO existe")

conn.close()
