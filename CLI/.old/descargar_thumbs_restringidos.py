import os
import asyncio
import random
import argparse
import aiosqlite
from pathlib import Path
from typing import List, Tuple, Dict

import sys
import os

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    DB_PATH,
    THUMB_FOLDER,
    FOLDER_SESSIONS,
    BOT_POOL_TOKENS,
    API_ID, API_HASH, SESSION_NAME,
    API_ID2, API_HASH2, SESSION_NAME2,
    BOT_WAIT_MIN, BOT_WAIT_MAX,
)
from utils import save_image_as_webp

# Objetivo: intentar descargar thumbs de grupos restringidos usando
# Bot1 -> UserBot principal -> UserBot2, en ese orden.
# Se respeta el índice de Bot1 pero se usa solo para descargar cuando tenga acceso.


async def ensure_dirs(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


async def get_pending(limit: int, restricted_only: bool = False) -> List[Tuple[str, str, int, int]]:
    """
    Obtener videos sin thumb.
    - Por defecto toma pendientes normales.
    - Con restricted_only, toma los marcados como dump_fail (no reenviables al canal cache).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        if restricted_only:
            query = """
                SELECT id, file_unique_id, message_id, chat_id
                FROM videos_telegram
                WHERE has_thumb = 0
                  AND dump_fail = 1
                  AND dump_message_id IS NULL
                ORDER BY fecha_mensaje DESC
                LIMIT ?
            """
            params = (limit,)
        else:
            query = """
                SELECT id, file_unique_id, message_id, chat_id
                FROM videos_telegram
                WHERE has_thumb = 0 AND message_id IS NOT NULL
                  AND (dump_fail IS NULL OR dump_fail = 0)
                ORDER BY fecha_mensaje DESC
                LIMIT ?
            """
            params = (limit,)

        async with db.execute(query, params) as cur:
            return await cur.fetchall()


async def marcar_completado(vid_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE videos_telegram SET has_thumb = 1 WHERE id = ?", (vid_id,))
        await db.commit()


async def marcar_fallido(vid_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE videos_telegram SET dump_fail = 1 WHERE id = ?", (vid_id,))
        await db.commit()


async def download_thumb(app: Client, chat_id: int, message_id: int, final_path: str) -> bool:
    msg = await app.get_messages(chat_id, message_id)
    media = msg.video or msg.document or msg.photo
    target_file_id = None
    if media:
        if getattr(media, "thumbs", None):
            target_file_id = media.thumbs[-1].file_id
        elif msg.photo:
            target_file_id = media.file_id
    if not target_file_id:
        return False

    tmp_dir = os.path.join(THUMB_FOLDER, "_tmp")
    await ensure_dirs(tmp_dir)
    tmp_name = os.path.join(tmp_dir, f"{message_id}_{random.randint(1000,9999)}")

    async def _verify_image(path: str):
        with Image.open(path) as img:
            img.verify()

    down = await app.download_media(target_file_id, file_name=tmp_name)
    if not down or not os.path.exists(down) or os.path.getsize(down) == 0:
        return False

    await asyncio.to_thread(_verify_image, down)
    await ensure_dirs(os.path.dirname(final_path))
    await asyncio.to_thread(save_image_as_webp, down, final_path)
    os.remove(down)
    return True


async def attempt_chain(clients: List[Tuple[str, Client]], tarea, stats: Dict[str, int], userbot2_no_access: Dict[str, int]):
    vid_id, unique_id, msg_id, chat_origin = tarea
    final_path = os.path.join(THUMB_FOLDER, str(chat_origin), f"{unique_id}.webp")

    if os.path.exists(final_path) and os.path.getsize(final_path) > 200:
        await marcar_completado(vid_id)
        return

    for name, app in clients:
        try:
            await asyncio.sleep(random.uniform(BOT_WAIT_MIN, BOT_WAIT_MAX))
            ok = await download_thumb(app, chat_origin, msg_id, final_path)
            if ok:
                stats[name] = stats.get(name, 0) + 1
                await marcar_completado(vid_id)
                return
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except RPCError as rpc_err:
            if name == "userbot2" and "ACCESS" in str(rpc_err).upper():
                userbot2_no_access["count"] = userbot2_no_access.get("count", 0) + 1
            continue
        except Exception:
            continue

    await marcar_fallido(vid_id)


async def main(limit: int, batch_size: int, batch_wait: float, restricted_only: bool):
    await ensure_dirs(FOLDER_SESSIONS)
    await ensure_dirs(THUMB_FOLDER)

    tareas = await get_pending(limit, restricted_only=restricted_only)
    if not tareas:
        print("Sin tareas pendientes")
        return

    # Clientes
    bot1_token = BOT_POOL_TOKENS[0]
    bot1_session = os.path.join(FOLDER_SESSIONS, "bot_worker_1")
    bot1 = Client(bot1_session, api_id=API_ID, api_hash=API_HASH, bot_token=bot1_token)

    user1_session = os.path.join(FOLDER_SESSIONS, SESSION_NAME)
    user1 = Client(user1_session, api_id=API_ID, api_hash=API_HASH)

    user2_session = os.path.join(FOLDER_SESSIONS, SESSION_NAME2)
    user2 = Client(user2_session, api_id=API_ID2, api_hash=API_HASH2)

    clients = [
        ("bot1", bot1),
        ("userbot1", user1),
        ("userbot2", user2),
    ]

    stats = {"bot1": 0, "userbot1": 0, "userbot2": 0}
    userbot2_no_access = {"count": 0}

    # Abrir sesiones
    for _, app in clients:
        await app.start()

    try:
        for i in range(0, len(tareas), batch_size):
            batch = tareas[i:i + batch_size]
            for tarea in batch:
                await attempt_chain(clients, tarea, stats, userbot2_no_access)
            if i + batch_size < len(tareas):
                await asyncio.sleep(batch_wait)
    finally:
        for _, app in clients:
            try:
                await app.stop()
            except Exception:
                pass

    # Reporte
    total = sum(stats.values())
    print("==== REPORTE DESCARGA THUMBS RESTRINGIDOS ====")
    print(f"Total tareas procesadas: {len(tareas)}")
    print(f"Descargados Bot1: {stats['bot1']}")
    print(f"Descargados UserBot1: {stats['userbot1']}")
    print(f"Descargados UserBot2: {stats['userbot2']}")
    print(f"Total descargados: {total}")
    print(f"UserBot2 sin acceso: {userbot2_no_access.get('count',0)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descargar thumbs de grupos restringidos usando bot + userbots")
    parser.add_argument("--limit", type=int, default=50, help="Máximo de tareas a procesar")
    parser.add_argument("--batch-size", type=int, default=5, help="Tamaño de lote")
    parser.add_argument("--batch-wait", type=float, default=2.0, help="Espera entre lotes (seg)")
    parser.add_argument("--restricted-only", action="store_true", help="Procesar solo los marcados como dump_fail (restringidos)")
    args = parser.parse_args()

    asyncio.run(main(args.limit, args.batch_size, args.batch_wait, args.restricted_only))
