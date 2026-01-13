import asyncio
import json
import os
import sys

from pyrogram import enums
from pyrogram.errors import FloodWait

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.telegram_client import get_client
from services.video_processor import procesar_mensaje_video
from database import (
    db_count_videos_by_chat,
)


async def scan_all_channels_to_db(
    max_videos_per_chat: int | None = None,
    max_indexed_videos_per_chat: int | None = None,
    only_chat_id: int | None = None,
):
    """Recorre todos los canales/supergrupos del usuario y guarda los videos en la BD.

    Usa las mismas tablas y formato que la app principal:
    - videos_telegram (db_upsert_video)
    - video_messages (db_upsert_video_message)
    - video_file_ids (db_add_video_file_id)
    """
    client = get_client()
    await client.start()
    print("✅ Sesión Pyrogram iniciada (usuario).")

    try:
        # Recorremos todos los diálogos de la cuenta
        async for dialog in client.get_dialogs():
            chat = dialog.chat

            # Solo canales y supergrupos
            if chat.type not in (enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP):
                continue
            
            if only_chat_id is not None and chat.id != only_chat_id:
                continue
            
            indexed_count = await db_count_videos_by_chat(chat.id)
            if max_indexed_videos_per_chat is not None and indexed_count > max_indexed_videos_per_chat:
                print(
                    f"\n⏭️ Saltando: {chat.title} (id={chat.id}) | videos indexados={indexed_count} > {max_indexed_videos_per_chat}"
                )
                continue

            print(f"\n📺 Escaneando canal/supergrupo: {chat.title} (id={chat.id})")
            count_new = 0

            try:
                async for m in client.search_messages(
                    chat_id=chat.id,
                    filter=enums.MessagesFilter.VIDEO,
                ):
                    if max_videos_per_chat is not None and count_new >= max_videos_per_chat:
                        break

                    resultado = await procesar_mensaje_video(m, origen="scan_all", incluir_raw_json=True)
                    
                    if resultado["procesado"]:
                        count_new += 1
                    else:
                        continue
                    if count_new % 50 == 0:
                        print(f"  · {count_new} videos procesados en {chat.title}...")

            except FloodWait as e:
                print(f"⏳ FloodWait de {e.value}s en {chat.id}, esperando...")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"⚠️ Error escaneando {chat.id} ({chat.title}): {e}")

            print(f"✅ Canal {chat.title} ({chat.id}): {count_new} videos guardados/actualizados.")

    finally:
        await client.stop()
        print("🛑 Cliente de Telegram detenido")


if __name__ == "__main__":
    # Puedes limitar videos por canal pasando un número en max_videos_per_chat
    raw = input(
        "Máximo de videos YA indexados en BD para escanear un canal (ENTER=sin filtro, 0=solo vacíos): "
    ).strip()

    if raw == "":
        max_indexed = None
    else:
        try:
            max_indexed = int(raw)
        except ValueError:
            raise SystemExit("Valor inválido. Ingresa un entero (ej: 0, 200) o ENTER.")

    asyncio.run(
        scan_all_channels_to_db(
            max_videos_per_chat=None,
            max_indexed_videos_per_chat=max_indexed
        )
    )
