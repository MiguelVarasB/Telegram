import asyncio
import json
import os
import sys
import aiofiles  # Requiere: pip install aiofiles
from pyrogram.errors import FloodWait

# Ajustar path para importar configuración del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.telegram_client import get_client
from config import JSON_FOLDER, ensure_directories

async def export_channel_to_json(target_chat_id: int | str):
    client = get_client()
    if not client.is_connected:
        await client.start()

    try:
        # 1. Obtener información del chat para el nombre del archivo
        chat = await client.get_chat(target_chat_id)
        chat_title = chat.title or chat.username or chat.first_name or "chat"
        safe_title = "".join([c for c in chat_title if c.isalnum() or c in (' ', '-', '_')]).strip()
        filename = f"FULL_DUMP_{safe_title}_{chat.id}.json"
        filepath = os.path.join(JSON_FOLDER, filename)
        
        ensure_directories()
        
        print(f"📂 Objetivo: {chat.title} (ID: {chat.id})")
        print(f"💾 Guardando en: {filepath}")
        print("🚀 Iniciando descarga (esto puede tardar)...")

        # 2. Escribir archivo usando "streaming" (línea por línea)
        # Esto crea un JSON Array válido [ {...}, {...} ] sin cargar todo en RAM
        async with aiofiles.open(filepath, mode="w", encoding="utf-8") as f:
            await f.write("[\n")  # Abrir array
            
            count = 0
            first_item = True
            
            async for message in client.get_chat_history(chat.id):
                if not first_item:
                    await f.write(",\n") # Coma separadora entre objetos
                
                # TRUCO: str(message) en Pyrogram devuelve un JSON string válido.
                # Lo convertimos a dict (json.loads) y volvemos a texto (json.dumps) 
                # para asegurar que se escriba en una sola línea (compacto) y con caracteres correctos.
                msg_dict = json.loads(str(message))
                
                # Escribimos el objeto minificado para ahorrar espacio
                await f.write(json.dumps(msg_dict, ensure_ascii=False))
                
                first_item = False
                count += 1
                
                if count % 100 == 0:
                    print(f"  · {count} mensajes exportados...", end="\r")

            await f.write("\n]") # Cerrar array
            
        print(f"\n✅ EXPORTACIÓN COMPLETADA.")
        print(f"📊 Total mensajes: {count}")
        print(f"📄 Archivo: {filepath}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    # Modo interactivo
    target = input("Ingresa el ID del canal, Username o Enlace de invitación: ").strip()
    
    # Intentar convertir a entero si es un ID numérico
    if target.lstrip("-").isdigit():
        target = int(target)
        
    asyncio.run(export_channel_to_json(target))