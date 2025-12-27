import asyncio
import os
import sys
from pyrogram import enums

# Ajuste de ruta para importar tus servicios
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.telegram_client import get_client

async def obtener_datos_canal():
    client = get_client()
    if not client.is_connected:
        await client.start()

    print("\n" + "="*50)
    print("🔍 BUSCADOR DE CANALES/GRUPOS PÚBLICOS + CONTEO")
    print("="*50)
    busqueda = input("👉 Ingresa el nombre o @username del canal: ").strip()

    try:
        # 1. Intentamos obtenerlo directamente si es un username (@ejemplo)
        if busqueda.startswith("@") or not " " in busqueda:
            print(f"📡 Buscando por username exacto: {busqueda}...")
            chat = await client.get_chat(busqueda)
            
            # Obtener el conteo de videos antes de mostrar la info
            video_count = await client.search_messages_count(chat.id, filter=enums.MessagesFilter.VIDEO)
            mostrar_info(chat, video_count)
        
        # 2. Si no es un username, buscamos globalmente por el nombre
        else:
            print(f"🔎 Buscando chats públicos con el nombre: '{busqueda}'...")
            resultados = await client.search_public_chats(busqueda)
            
            if not resultados:
                print("❌ No se encontraron canales públicos con ese nombre.")
                return

            print(f"\n✅ Se encontraron {len(resultados)} resultados:")
            for i, chat in enumerate(resultados):
                username = f"@{chat.username}" if chat.username else "Sin username"
                print(f"[{i}] {chat.title} ({username}) - ID: {chat.id}")

            seleccion = input("\n📝 Selecciona el número para ver detalles (o 'n' para salir): ")
            if seleccion.isdigit() and int(seleccion) < len(resultados):
                # Pedimos el chat completo para obtener el conteo de miembros y descripción
                chat_detalle = await client.get_chat(resultados[int(seleccion)].id)
                
                # Obtener el conteo de videos
                video_count = await client.search_messages_count(chat_detalle.id, filter=enums.MessagesFilter.VIDEO)
                mostrar_info(chat_detalle, video_count)

    except Exception as e:
        print(f"❌ Error al buscar: {e}")
    finally:
        await client.stop()

def mostrar_info(chat, video_count):
    print("\n" + "📊 INFORMACIÓN DETALLADA")
    print("━" * 40)
    print(f"📛 Título:      {chat.title}")
    print(f"🆔 ID:          {chat.id}")
    print(f"🎥 VIDEOS:      {video_count}  <--") # Nueva línea de conteo
    print(f"👤 Username:    @{chat.username if chat.username else 'N/A'}")
    print(f"📂 Tipo:        {chat.type}")
    print(f"👥 Miembros:    {chat.members_count if chat.members_count else 'No visible'}")
    print(f"📝 Descripción: {chat.description if chat.description else 'Sin descripción'}")
    
    if chat.linked_chat:
        print(f"🔗 Chat vinculado: {chat.linked_chat.title} (ID: {chat.linked_chat.id})")
    
    print("━" * 40)

if __name__ == "__main__":
    asyncio.run(obtener_datos_canal())