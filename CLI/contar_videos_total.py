import asyncio
import os
import sys
import datetime
from pyrogram import enums
from pyrogram.errors import FloodWait

# Ajuste de ruta para tus servicios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client
from database import init_db, db_upsert_chat_basic, db_upsert_chat_video_count

async def contar_videos_en_todos_mis_chats():
    await init_db()
    client = get_client()
    if not client.is_connected:
        await client.start()

    print("\n" + "="*70)
    print("🎥 AUDITORÍA DE VIDEOS POR CANAL (MIS CHATS)")
    print("="*70)
    print("📡 Analizando diálogos... Por favor espera.\n")
    
    resultados = []
    
    try:
        # 1. Obtenemos todos los diálogos sin límite para analizar la cuenta completa
        async for dialog in client.get_dialogs():
            chat = dialog.chat
            
            # 2. Filtramos solo Grupos, Supergrupos y Canales
            if chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP, enums.ChatType.GROUP]:
                try:
                    # 3. Contamos los videos en este chat específico
                    count = await client.search_messages_count(
                        chat.id, 
                        filter=enums.MessagesFilter.VIDEO
                    )

                    # 3.1 Guardamos/actualizamos chat y conteo en BD
                    scanned_at = datetime.datetime.utcnow().isoformat()
                    titulo = chat.title or getattr(chat, "first_name", None) or "Sin Nombre"
                    username = getattr(chat, "username", None)
                    await db_upsert_chat_basic(
                        chat_id=chat.id,
                        name=titulo,
                        chat_type=str(chat.type).replace("ChatType.", ""),
                        username=username,
                        raw_json=str(chat),
                    )
                    await db_upsert_chat_video_count(chat.id, count, scanned_at)
                    
                    if count > 0:
                        resultados.append({
                            "titulo": titulo[:35],
                            "id": chat.id,
                            "videos": count
                        })
                        print(f"✅ Procesado: {titulo[:30]}... ({count} videos)")
                    else:
                        print(f"ℹ️ Procesado: {titulo[:30]}... (0 videos)")
                        
                except FloodWait as e:
                    # Si Telegram nos frena, esperamos lo que pida
                    print(f"⚠️ Esperando {e.value} segundos por límite de Flood...")
                    await asyncio.sleep(e.value)
                except Exception as err:
                    print(f"❌ Error en chat {chat.id}: {err}")
                    continue

        # 4. Mostrar reporte final ordenado por cantidad de videos
        print("\n" + "📊 REPORTE FINAL (Ordenado por volumen)")
        print("-" * 75)
        print(f"{'CANAL / GRUPO':<35} | {'VIDEOS':<10} | {'ID'}")
        print("-" * 75)
        
        # Ordenamos de mayor a menor cantidad de videos
        resultados.sort(key=lambda x: x['videos'], reverse=True)
        
        for res in resultados:
            print(f"{res['titulo']:<35} | {res['videos']:<10} | {res['id']}")
            
        print("-" * 75)
        print(f"✅ Total de canales con videos: {len(resultados)}")

    except Exception as e:
        print(f"❌ Error general: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(contar_videos_en_todos_mis_chats())