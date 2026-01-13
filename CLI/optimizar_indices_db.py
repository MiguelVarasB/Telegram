import sqlite3
import sys
from pathlib import Path
from utils import log_timing
"""
Script para crear índices optimizados en la base de datos.
Estos índices aceleran las consultas más frecuentes del pipeline.
"""

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config import DB_PATH


def crear_indices_optimizados():
    """Crea índices para acelerar las consultas más comunes."""
    db_path = Path(DB_PATH)
    if not db_path.exists():
        log_timing(f"No existe la BD en {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        
        indices = [
            # Índice para búsquedas por chat_id en video_messages (muy usado)
            ("idx_video_messages_chat_id", 
             "CREATE INDEX IF NOT EXISTS idx_video_messages_chat_id ON video_messages(chat_id)"),
            
            # Índice compuesto para verificación de existencia de mensajes
            ("idx_video_messages_chat_msg", 
             "CREATE INDEX IF NOT EXISTS idx_video_messages_chat_msg ON video_messages(chat_id, message_id)"),
            
            # Índice para contar duplicados por video_id y chat_id
            ("idx_video_messages_video_chat", 
             "CREATE INDEX IF NOT EXISTS idx_video_messages_video_chat ON video_messages(video_id, chat_id)"),
            
            # Índice para filtrar chats activos no propios
            ("idx_chats_activo_owner", 
             "CREATE INDEX IF NOT EXISTS idx_chats_activo_owner ON chats(activo, is_owner)"),
            
            # Índice para ordenar por last_message_date
            ("idx_chats_last_message", 
             "CREATE INDEX IF NOT EXISTS idx_chats_last_message ON chats(last_message_date)"),
            
            # Índice para chat_video_counts por chat_id (clave primaria si no existe)
            ("idx_chat_video_counts_chat_id", 
             "CREATE INDEX IF NOT EXISTS idx_chat_video_counts_chat_id ON chat_video_counts(chat_id)"),
        ]
        
        log_timing("Creando índices optimizados...")
        for nombre, sql in indices:
            try:
                cur.execute(sql)
                log_timing(f"  ✅ {nombre}")
            except sqlite3.OperationalError as e:
                if "already exists" in str(e):
                    log_timing(f"  ℹ️  {nombre} (ya existe)")
                else:
                    log_timing(f"  ❌ {nombre}: {e}")
        
        conn.commit()
        
        # Analizar la base de datos para optimizar el query planner
        log_timing("\nAnalizando base de datos para optimizar el query planner...")
        cur.execute("ANALYZE")
        conn.commit()
        log_timing("  ✅ Análisis completado")
        
        # Mostrar estadísticas de índices
        log_timing("\nEstadísticas de índices:")
        cur.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index' ORDER BY tbl_name, name")
        indices_existentes = cur.fetchall()
        
        tablas = {}
        for idx_name, tbl_name in indices_existentes:
            if tbl_name not in tablas:
                tablas[tbl_name] = []
            tablas[tbl_name].append(idx_name)
        
        for tabla, indices in sorted(tablas.items()):
            log_timing(f"  📊 {tabla}: {len(indices)} índices")
        
        log_timing("\n✅ Optimización de índices completada")
        
    finally:
        conn.close()


if __name__ == "__main__":
    crear_indices_optimizados()
