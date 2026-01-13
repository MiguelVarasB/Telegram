import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from pyrogram import enums
from pyrogram.errors import FloodWait

# Permitir importar módulos del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.telegram_client import get_client  # noqa: E402
from services.video_processor import procesar_mensaje_video  # noqa: E402

DIAS_ATRAS = 5

async def listar_videos_recientes(dias: int):
    """Obtiene videos de todos los canales/grupos de los últimos `dias`."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=dias)
    client = get_client()
    await client.start()
    print(f"✅ Sesión Pyrogram iniciada. Buscando videos desde {cutoff.isoformat()}")

    resultados = []
    total_videos = 0

    try:
        async for dialog in client.get_dialogs():
            chat = dialog.chat
            if chat.type not in (
                enums.ChatType.CHANNEL,
                enums.ChatType.SUPERGROUP,
                enums.ChatType.GROUP,
            ):
                continue

            print(f"\n📺 Revisando {chat.title} (id={chat.id})")
            count_chat = 0

            try:
                async for m in client.search_messages(
                    chat_id=chat.id,
                    filter=enums.MessagesFilter.VIDEO,
                ):
                    if not m.date:
                        continue

                    # Los mensajes vienen de más reciente a más antiguo
                    if m.date.replace(tzinfo=timezone.utc) < cutoff:
                        break

                    if not m.video:
                        continue

                    v = m.video
                    resultados.append(
                        {
                            "chat_id": chat.id,
                            "chat_title": chat.title,
                            "message_id": m.id,
                            "date": m.date.isoformat(),
                            "file_id": v.file_id,
                            "file_unique_id": v.file_unique_id,
                            "file_name": v.file_name,
                            "duration": v.duration,
                            "width": v.width,
                            "height": v.height,
                            "size_bytes": v.file_size,
                            "caption": m.caption,
                            "views": m.views,
                            "outgoing": m.outgoing,
                        }
                    )
                    count_chat += 1
                    total_videos += 1

                    # Guardar en base de datos usando el servicio centralizado
                    await procesar_mensaje_video(m, origen="listar_recientes")

                    if count_chat % 25 == 0:
                        print(f"  · {count_chat} videos recientes encontrados en {chat.title}")

            except FloodWait as e:
                print(f"⏳ FloodWait de {e.value}s en {chat.id}, esperando...")
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"⚠️ Error en {chat.id} ({chat.title}): {e}")

            print(f"✅ {chat.title}: {count_chat} videos dentro del rango.")

    finally:
        await client.stop()
        print("🛑 Cliente de Telegram detenido")

    print(f"\n🎉 Total videos recientes encontrados: {total_videos}")
    return resultados


async def main():
    parser = argparse.ArgumentParser(
        description="Listar videos de canales/grupos de los últimos N días."
    )
    parser.add_argument(
        "--dias", type=int, default=DIAS_ATRAS, help="Cantidad de días hacia atrás (default: {DIAS_ATRAS})"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Ruta de archivo JSON para guardar resultados (opcional).",
    )
    args = parser.parse_args()

    resultados = await listar_videos_recientes(args.dias)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        print(f"💾 Resultados guardados en {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
