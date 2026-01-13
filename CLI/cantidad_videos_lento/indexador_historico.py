import asyncio
import os
import sys
from pathlib import Path

import aiosqlite
from pyrogram.errors import FloodWait

# --- GESTIÓN DE RUTAS ROBUSTA ---
BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from config import DB_PATH, CACHE_DUMP_VIDEOS_CHANNEL_ID
    from services.telegram_client import get_client
    # Importamos la versión BATCH del procesador
    from services.video_processor import procesar_mensajes_video_batch 
    from database.chats import db_get_chat
    from utils.database_helpers import ensure_column
    from utils import log_timing
except ImportError as e:
    print(f"❌ Error de importación: {e}")
    sys.exit(1)


Excluir = [CACHE_DUMP_VIDEOS_CHANNEL_ID, -1002670762200, -1002621670240]

async def obtener_chats_con_historial(db):
    """
    Obtiene chats activos con videos, excluyendo aquellos que ya 
    alcanzaron el inicio histórico (is_first_video = 1).
    """
    # Aseguramos que existan las columnas necesarias
    await ensure_column(db, "chat_video_counts", "last_hist_scan", "TEXT", "NULL")
    await ensure_column(db, "video_messages", "is_first_video", "INTEGER", "0")
    
    async with db.execute(
        """
        SELECT cvc.chat_id, c.name
        FROM chat_video_counts cvc
        JOIN chats c ON cvc.chat_id = c.chat_id
        WHERE c.activo = 1 
          AND cvc.videos_count > 0
          AND COALESCE(c.is_owner, 0) = 0
          AND cvc.chat_id NOT IN (
              SELECT chat_id FROM video_messages WHERE is_first_video = 1
          )
        ORDER BY cvc.videos_count DESC
        """
    ) as cursor:
        return await cursor.fetchall()

async def escanear_historia_antigua(client, db, chat_id, chat_nombre):
    """
    Procesa el historial hacia atrás y marca el 'piso' del canal 
    para no volver a escanearlo nunca más.
    """
    if chat_id in Excluir: return

    # 1. Buscamos el ID más antiguo registrado para empezar desde ahí hacia atrás
    async with db.execute(
        "SELECT message_id FROM video_messages WHERE chat_id = ? ORDER BY message_id ASC LIMIT 1", 
        (chat_id,)
    ) as cur:
        row = await cur.fetchone()
        offset_id = row[0] if row else None

    if not offset_id:
        return 

    log_timing(f"\n🚀 {chat_nombre} | Continuando hacia atrás desde ID: {offset_id}")

    stats = {"videos": 0, "fotos": 0, "documentos": 0, "otros": 0, "nuevos_db": 0}
    count_total = 0
    
    # Buffer para batch processing
    batch_videos = []
    BATCH_SIZE = 50

    async def procesar_lote_pendiente():
        """Función auxiliar para procesar el buffer actual."""
        if not batch_videos: return
        
        # Procesamos 50 videos de una sola vez (1 transacción DB en lugar de 50)
        resultados = await procesar_mensajes_video_batch(batch_videos, origen="historico")
        
        # Actualizamos estadísticas
        for res in resultados:
            if res.get("procesado") and res.get("video_nuevo"):
                stats["nuevos_db"] += 1
        
        stats["videos"] += len(batch_videos)
        batch_videos.clear()

    try:
        # Escaneo principal
        async for message in client.get_chat_history(chat_id, offset_id=offset_id):
            count_total += 1
            
            if message.video:
                batch_videos.append(message)
                
                # Si llenamos el buffer, procesamos
                if len(batch_videos) >= BATCH_SIZE:
                    await procesar_lote_pendiente()
                    
            elif message.photo: stats["fotos"] += 1
            elif message.document: stats["documentos"] += 1
            else: stats["otros"] += 1
            
            # Log de progreso y commit periódico de la conexión principal
            if count_total % 200 == 0:
                # Procesamos lo que quede pendiente antes de reportar
                await procesar_lote_pendiente()
                
                # Commit de la conexión principal (para el flag last_hist_scan si se usara)
                await db.commit() 
                log_timing(f"  📥 Leídos: {count_total} | 🎥 V: {stats['videos']} | ✨ +{stats['nuevos_db']}", end="\r")

        # Procesar remanentes al salir del bucle
        await procesar_lote_pendiente()

        # --- CHEQUEO FINAL DE SEGURIDAD ---
        # Volvemos a pedir el historial desde el último punto alcanzado para confirmar vacío
        async with db.execute("SELECT MIN(message_id) FROM video_messages WHERE chat_id = ?", (chat_id,)) as cur:
            last_min_row = await cur.fetchone()
            piso_id = last_min_row[0] if last_min_row else None

        confirmacion_vacio = True
        if piso_id:
            async for _ in client.get_chat_history(chat_id, offset_id=piso_id, limit=5):
                confirmacion_vacio = False # Si entra aquí, es que Telegram aún tenía algo
                break

            if confirmacion_vacio:
                # Marcamos el video más antiguo de este chat como el "Primero"
                await db.execute(
                    "UPDATE video_messages SET is_first_video = 1 WHERE chat_id = ? AND message_id = ?",
                    (chat_id, piso_id)
                )
                log_timing(f"📍 Marcado ID {piso_id} como ORIGEN (piso) de {chat_nombre}")

        # Actualizar timestamp de escaneo
        await db.execute(
            "UPDATE chat_video_counts SET last_hist_scan = datetime('now') WHERE chat_id = ?",
            (chat_id,)
        )
        await db.commit()
        
        log_timing(f"\n✅ FIN {chat_nombre} | Videos: {stats['videos']} | Fotos: {stats['fotos']}")

    except FloodWait as fw:
        log_timing(f"\n⚠️ FloodWait: {fw.value}s")
        await asyncio.sleep(fw.value)
    except Exception as e:
        log_timing(f"\n❌ Error: {e}")

async def main():
    client = get_client(clone_for_cli=True)
    await client.start()
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Optimizaciones importantes para escritura masiva
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous = OFF") # Arriesgado si se va la luz, pero muy rápido
        
        chats = await obtener_chats_con_historial(db)
        log_timing(f"🔥 Analizando historial de {len(chats)} canales...")

        for chat_id, chat_nombre in chats:
            await escanear_historia_antigua(client, db, chat_id, chat_nombre)

    await client.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Detenido.")