import asyncio
import json
import os
import sys

from pyrogram import enums
from pyrogram.errors import FloodWait

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.telegram_client import get_client
from database import (
    db_upsert_video,
    db_add_video_file_id,
    db_upsert_video_message,
    db_count_videos_by_chat,
)


async def scan_all_channels_to_db(
    max_videos_per_chat: int | None = None,
    max_indexed_videos_per_chat: int | None = None,
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

                    if not m.video:
                        continue

                    v = m.video
                    fn = v.file_name or f"Video {m.id}"
                    msg_dict = json.loads(str(m))

                    # ---- VIDEO PRINCIPAL (videos_telegram) ----
                    video_data = {
                        "chat_id": chat.id,
                        "message_id": m.id,
                        "file_id": v.file_id,
                        "file_unique_id": v.file_unique_id,
                        "nombre": fn,
                        "caption": m.caption,
                        "tamano_bytes": v.file_size,
                        "fecha_mensaje": m.date.isoformat() if m.date else None,
                        "duracion": v.duration or 0,
                        "ancho": v.width or 0,
                        "alto": v.height or 0,
                        "mime_type": v.mime_type,
                        "views": m.views or 0,
                        "outgoing": m.outgoing,
                    }
                    await db_upsert_video(video_data)
                    await db_add_video_file_id(
                        v.file_unique_id, v.file_id, v.file_unique_id, "scan_all"
                    )

                    # ---- METADATOS EXTENDIDOS DEL MENSAJE (video_messages) ----
                    message_data = {
                        "video_id": v.file_unique_id,
                        "chat_id": chat.id,
                        "message_id": m.id,
                        "date": m.date.isoformat() if m.date else None,
                        "from_user": msg_dict.get("from_user"),
                        "media": msg_dict.get("media"),
                        "views": m.views or 0,
                        "forwards": getattr(m, "forwards", None),
                        "outgoing": m.outgoing,
                        "reply_to_message_id": getattr(m, "reply_to_message_id", None),
                        "forward_from_chat": msg_dict.get("forward_from_chat"),
                        "forward_from_message_id": msg_dict.get("forward_from_message_id"),
                        "forward_date": msg_dict.get("forward_date"),
                        "caption": msg_dict.get("caption"),
                        "caption_entities": msg_dict.get("caption_entities"),
                        "raw_json": msg_dict,
                    }
                    await db_upsert_video_message(message_data)

                    count_new += 1
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
            max_indexed_videos_per_chat=max_indexed,
        )
    )
