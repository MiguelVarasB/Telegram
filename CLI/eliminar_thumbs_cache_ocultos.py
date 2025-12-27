import os
import asyncio
import aiosqlite
from config import DB_PATH, THUMB_FOLDER, CACHE_DIR

async def eliminar_archivos_ocultos():
    """
    Elimina thumbnails y archivos de caché para videos marcados como ocultos (>=2)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id, chat_id, file_unique_id
            FROM videos_telegram
            WHERE oculto >= 2
            """
        ) as cursor:
            videos = await cursor.fetchall()

    thumb_eliminados = 0
    cache_eliminados = 0

    for video in videos:
        video_id, chat_id, file_unique_id = video
        
        # Eliminar thumbnail
        thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{file_unique_id}.webp")
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
            thumb_eliminados += 1

        # Eliminar archivos de caché
        cache_path = os.path.join(CACHE_DIR, f"{file_unique_id}.cache")
        if os.path.exists(cache_path):
            os.remove(cache_path)
            cache_eliminados += 1

    return thumb_eliminados, cache_eliminados

if __name__ == "__main__":
    thumb, cache = asyncio.run(eliminar_archivos_ocultos())
    print(f"Eliminados: {thumb} thumbnails y {cache} archivos de caché")
