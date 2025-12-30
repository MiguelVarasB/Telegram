import asyncio
import os

import aiosqlite

from config import DB_PATH, THUMB_FOLDER


THUMB_EXT = ".webp"


def build_thumb_path(chat_id: int, video_id: str) -> str:
    return os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}{THUMB_EXT}")


async def marcar_thumbs_en_disco(update_db: bool = True) -> None:
    """Busca videos con has_thumb=0 y marca en DB si el archivo existe en disco."""
    encontrados = 0
    actualizados = 0
    faltantes = 0

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, chat_id FROM videos_telegram WHERE has_thumb = 0"
        ) as cursor:
            rows = await cursor.fetchall()

        for video_id, chat_id in rows:
            path = build_thumb_path(chat_id, video_id)
            if os.path.exists(path):
                encontrados += 1
                if update_db:
                    await db.execute(
                        "UPDATE videos_telegram SET has_thumb = 1 WHERE id = ?",
                        (video_id,),
                    )
                    actualizados += 1
            else:
                faltantes += 1

        if update_db and actualizados:
            await db.commit()

    print(f"Total revisados: {len(rows)}")
    print(f"Con thumb en disco: {encontrados}")
    if update_db:
        print(f"Actualizados en DB: {actualizados}")
    print(f"Sin thumb en disco: {faltantes}")


def main():
    asyncio.run(marcar_thumbs_en_disco(update_db=True))


if __name__ == "__main__":
    main()
