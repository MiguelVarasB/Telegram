import asyncio
import os
import sys
from pyrogram import enums, raw

# Ajuste de ruta para importar tus servicios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client

async def buscador_global_full():
    client = get_client()
    if not client.is_connected:
        await client.start()

    print("\n" + "="*50)
    print("🔍 MEGA-BUSCADOR GLOBAL + CONTEO DE VIDEOS")
    print("="*50)
    query = input("👉 Ingresa palabra clave para buscar: ").strip()

    if len(query) < 3:
        print("⚠️ Ingresa al menos 3 caracteres.")
        await client.stop()
        return

    try:
        print(f"📡 Consultando servidores globales de Telegram...")
        # Búsqueda RAW para encontrar canales nuevos fuera de tus diálogos
        resultado = await client.invoke(
            raw.functions.contacts.Search(
                q=query,
                limit=20
            )
        )

        canales = resultado.chats
        if not canales:
            print("❌ No se encontraron resultados públicos.")
            return

        print(f"\n✅ Canales encontrados:")
        print("-" * 80)
        print(f"{'N°':<4} | {'TÍTULO':<35} | {'USERNAME':<18}")
        print("-" * 80)

        for i, chat in enumerate(canales):
            title = getattr(chat, "title", "Sin título")
            username = f"@{chat.username}" if getattr(chat, "username", None) else "N/A"
            print(f"[{i:<2}] | {title[:35]:<35} | {username:<18}")

        print("-" * 80)
        opcion = input("\n📝 Selecciona el número para ver el reporte detallado (o 'q' para salir): ")
        
        if opcion.isdigit() and int(opcion) < len(canales):
            chat_data = canales[int(opcion)]
            
            # Resolvemos el Peer para evitar errores de ID inválido
            # Si tiene username lo usamos, si no, usamos el ID directamente
            identificador = chat_data.username if getattr(chat_data, "username", None) else chat_data.id
            
            print(f"🔄 Generando reporte para: {identificador}...")
            
            # Obtenemos el objeto Chat completo (fat object)
            chat_obj = await client.get_chat(identificador)
            
            # Realizamos el conteo de videos en tiempo real
            video_count = await client.search_messages_count(chat_obj.id, filter=enums.MessagesFilter.VIDEO)
            
            # Mostramos la información con el formato que ya te funciona bien
            mostrar_info_detallada(chat_obj, video_count)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.stop()

def mostrar_info_detallada(chat, video_count):
    print("\n" + "📊 INFORMACIÓN DETALLADA")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"📛 Título:      {chat.title}")
    print(f"🆔 ID:          {chat.id}")
    print(f"🎥 VIDEOS:      {video_count}  <--")
    print(f"👤 Username:    @{chat.username if chat.username else 'N/A'}")
    print(f"📂 Tipo:        {chat.type}")
    print(f"👥 Miembros:    {chat.members_count if chat.members_count else 'No visible'}")
    print(f"📝 Descripción: {chat.description[:150] + '...' if chat.description else 'Sin descripción'}")
    
    if chat.linked_chat:
        print(f"🔗 Chat vinculado: {chat.linked_chat.title} (ID: {chat.linked_chat.id})")
    
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == "__main__":
    asyncio.run(buscador_global_full())