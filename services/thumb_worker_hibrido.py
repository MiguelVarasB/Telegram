"""
Worker Híbrido INTELIGENTE:
1. Intenta descargar con el BOT (Rápido, sin límites).
2. Si el Bot no tiene acceso, usa el USERBOT (Lento, modo seguro).
"""
import os
import asyncio
import aiosqlite
from contextlib import AsyncExitStack
from pyrogram import Client
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired, PeerIdInvalid, FileReferenceExpired

from config import DB_PATH, THUMB_FOLDER, API_ID, API_HASH, BOT_POOL_TOKENS, CACHE_DUMP_VIDEOS_CHANNEL_ID
from .telegram_client import get_client

from utils import save_image_as_webp

# Configuración
CONCURRENCY_BOT = 10     # El Bot puede ir rápido
CONCURRENCY_USER = 1     # El Usuario debe ir lento (Modo Seguro)

async def _get_videos_pendientes() -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id, file_unique_id, dump_message_id, chat_id, message_id
            FROM videos_telegram
            WHERE has_thumb = 0
              AND (dump_fail IS NULL OR dump_fail = 0)
            """
        ) as cursor:
            return await cursor.fetchall()

async def _marcar_listo(video_id):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE videos_telegram SET has_thumb = 1 WHERE id = ?", (video_id,))
            await db.commit()
    except: pass

async def _descargar_con_cliente(client, chat_id, message_id, file_unique_id, folder, final_path, es_bot=False):
    """Lógica genérica de descarga para reusar en Bot y User."""

    async def _intentar_descarga():
        msg = await client.get_messages(chat_id, message_id)
        if not msg:
            return False

        media = getattr(msg, "video", None) or getattr(msg, "document", None)
        if not media:
            return False

        thumbs = getattr(media, "thumbs", None)
        thumb = thumbs[-1] if thumbs else getattr(media, "thumb", None)
        if not thumb:
            return False

        tmp_dir = os.path.join(THUMB_FOLDER, "_tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_path = os.path.join(tmp_dir, f"{chat_id}_{message_id}_{file_unique_id}_{'bot' if es_bot else 'user'}.jpg")

        down_path = await client.download_media(thumb.file_id, file_name=tmp_path)
        if not down_path:
            return False

        await asyncio.to_thread(save_image_as_webp, down_path, final_path)

        if os.path.exists(down_path):
            os.remove(down_path)

        if os.path.exists(final_path) and os.path.getsize(final_path) > 100:
            return True

        return False

    try:
        res = await _intentar_descarga()
        if res is True:
            return True
    except FileReferenceExpired:
        # Refrescar file_id reobteniendo el mensaje y reintentando una vez
        try:
            res = await _intentar_descarga()
            if res is True:
                return True
        except FileReferenceExpired:
            return False
        except Exception:
            return False
    except (ChannelPrivate, ChatAdminRequired, PeerIdInvalid):
        return "SIN_ACCESO"  # Señal para cambiar de estrategia
    except FloodWait as e:
        print(f"⏳ FloodWait ({'BOT' if es_bot else 'USER'}) {e.value}s")
        await asyncio.sleep(e.value)
        return False
    except Exception:
        return False

    return False

async def _procesar_hibrido(sem_bot, bot_clients, row, idx):
    id_video, file_unique_id, dump_message_id, chat_origin, message_id = row
    folder = os.path.join(THUMB_FOLDER, str(chat_origin))
    final_path = os.path.join(folder, f"{file_unique_id}.webp")

    os.makedirs(folder, exist_ok=True)

    # Si ya existe, saltar
    if os.path.exists(final_path) and os.path.getsize(final_path) > 1000:
        await _marcar_listo(id_video)
        return

    # Si tenemos dump_message_id (>0) usamos los bots contra el canal de dump
    if dump_message_id and dump_message_id > 0 and bot_clients:
        start_at = idx % len(bot_clients)
        ordered_bots = bot_clients[start_at:] + bot_clients[:start_at]

        for bot_client in ordered_bots:
            async with sem_bot:
                res = await _descargar_con_cliente(
                    bot_client,
                    CACHE_DUMP_VIDEOS_CHANNEL_ID,
                    dump_message_id,
                    file_unique_id,
                    folder,
                    final_path,
                    es_bot=True,
                )

            if res is True:
                await _marcar_listo(id_video)
                print(f"🤖 [BOT] Thumb OK: {file_unique_id}")
                return

        # Si llega acá, fallaron todos los bots para este dump_message_id.
        return

    # Sin dump_message_id: usar userbot directo al chat original
    try:
        user_client = get_client(use_server_session=True)
        if not user_client.is_connected:
            await user_client.start()

        res = await _descargar_con_cliente(
            user_client,
            chat_origin,
            message_id,
            file_unique_id,
            folder,
            final_path,
            es_bot=False,
        )

        if res is True:
            await _marcar_listo(id_video)
            print(f"👤 [USER] Thumb OK: {file_unique_id}")
    except Exception as e:
        print(f"⚠️ [USER] No se pudo descargar thumb {file_unique_id}: {e}")

async def background_thumb_downloader():
    print("🚀 Iniciando Worker Híbrido (Bot + User)...")
    try:
        filas = await _get_videos_pendientes()
        if not filas:
            print("✅ No hay pendientes.")
            return
        
        async with AsyncExitStack() as stack:
            bot_clients: list[Client] = []
            for i, token in enumerate(BOT_POOL_TOKENS, start=1):
                try:
                    bot_client = Client(
                        f"worker_bot_{i}",
                        api_id=API_ID,
                        api_hash=API_HASH,
                        bot_token=token,
                        in_memory=True,
                        no_updates=True,
                    )
                    await stack.enter_async_context(bot_client)
                    bot_clients.append(bot_client)
                except Exception as e:
                    print(f"⚠️ [BOT {i}] No pudo iniciar: {e}")

            if not bot_clients:
                print("❌ No hay bots disponibles para descargar thumbs.")
                return

            sem_bot = asyncio.Semaphore(CONCURRENCY_BOT * len(bot_clients))

            tasks = []
            for idx, row in enumerate(filas):
                tasks.append(_procesar_hibrido(sem_bot, bot_clients, row, idx))

            await asyncio.gather(*tasks)
        
        print("🏁 Worker Híbrido finalizado.")
    except asyncio.CancelledError:
        # Salir silenciosamente en apagados del servidor o cancelaciones explícitas
        print("⚠️ Worker Híbrido cancelado.")
    except Exception as e:
        print(f"❌ Worker Híbrido error: {e}")