import asyncio
import datetime
import os
import sys
import time

from pyrogram.raw.functions.messages import GetDialogs
from pyrogram.raw.types import (
    InputPeerChannel,
    InputPeerChat,
    InputPeerEmpty,
    InputPeerUser,
    PeerChannel,
    PeerChat,
    PeerUser,
)

# Asegurar importaciones del proyecto (misma estructura que otros CLI)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import ensure_directories
from database.connection import get_connection
from services import get_client

# --- CLIENTE GLOBAL ---
client = get_client()

# --- CONFIGURACIÓN ---
HORAS_ATRAS = 24  # 24 horas atrás desde AHORA
TIMESTAMP_LIMITE = time.time() - (HORAS_ATRAS * 3600)

print(f"📅 Fecha de corte: {datetime.datetime.fromtimestamp(TIMESTAMP_LIMITE)}")


# --- HELPERS (Reutilizados) ---
def formatear_fecha(timestamp):
    if not timestamp:
        return None
    if isinstance(timestamp, int):
        dt = datetime.datetime.fromtimestamp(timestamp)
    else:
        dt = timestamp
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def guardar_en_bd(chat_id, name, type_str, username, raw_json, last_date, folder_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chats (chat_id, name, type, username, raw_json, updated_at, last_message_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            name=excluded.name,
            type=excluded.type,
            username=excluded.username,
            updated_at=excluded.updated_at,
            last_message_date=excluded.last_message_date
        """,
        (
            chat_id,
            name,
            type_str,
            username,
            raw_json,
            datetime.datetime.now().isoformat(),
            last_date,
        ),
    )
    cur.execute(
        "INSERT OR IGNORE INTO chat_folders (chat_id, folder_id) VALUES (?, ?)",
        (chat_id, folder_id),
    )
    conn.commit()
    conn.close()


def get_next_offset_peer(peer, users_map, chats_map):
    try:
        if isinstance(peer, PeerUser):
            user = users_map.get(peer.user_id)
            if user:
                return InputPeerUser(user_id=user.id, access_hash=user.access_hash)
        elif isinstance(peer, PeerChannel):
            channel = chats_map.get(peer.channel_id)
            if channel:
                return InputPeerChannel(
                    channel_id=channel.id, access_hash=channel.access_hash
                )
        elif isinstance(peer, PeerChat):
            return InputPeerChat(chat_id=peer.chat_id)
    except Exception as e:
        print(f"⚠️ Warning construyendo peer: {e}")
    return InputPeerEmpty()


async def sincronizar_incremental(folder_id):
    nombre = "INBOX (0)" if folder_id == 0 else "ARCHIVADOS (1)"
    print(f"🔄 Escaneando cambios recientes en {nombre}...")

    offset_date = 0
    offset_id = 0
    offset_peer = InputPeerEmpty()
    limit = 100
    procesados = 0
    detener = False

    while not detener:
        try:
            raw = await client.invoke(
                GetDialogs(
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_peer=offset_peer,
                    limit=limit,
                    hash=0,
                    folder_id=folder_id,
                )
            )

            if not raw.dialogs:
                break

            chats_map = {c.id: c for c in raw.chats}
            users_map = {u.id: u for u in raw.users}
            messages_map = {m.id: m for m in raw.messages}

            for d in raw.dialogs:
                top_msg = messages_map.get(d.top_message)
                msg_date = top_msg.date if top_msg else 0

                is_pinned = getattr(d, "pinned", False)

                # Stop si encontramos chat viejo (no fijado)
                if not is_pinned and msg_date < TIMESTAMP_LIMITE:
                    print(
                        f"   🛑 Encontrado chat antiguo ({formatear_fecha(msg_date)}). Deteniendo escaneo."
                    )
                    detener = True
                    break

                if msg_date < TIMESTAMP_LIMITE:
                    continue

                p = d.peer
                entity = None
                chat_id = 0
                type_str = "UNKNOWN"

                if isinstance(p, PeerUser):
                    entity = users_map.get(p.user_id)
                    chat_id = p.user_id
                    type_str = "PRIVATE"
                elif isinstance(p, PeerChat):
                    entity = chats_map.get(p.chat_id)
                    chat_id = int(f"-{p.chat_id}")
                    type_str = "GROUP"
                elif isinstance(p, PeerChannel):
                    entity = chats_map.get(p.channel_id)
                    chat_id = int(f"-100{p.channel_id}")
                    type_str = getattr(entity, "megagroup", False) and "SUPERGROUP" or "CHANNEL"

                if not entity:
                    continue

                title = getattr(entity, "title", None) or f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()
                username = getattr(entity, "username", None)

                try:
                    raw_str = str(entity)
                except Exception:
                    raw_str = "{}"

                guardar_en_bd(
                    chat_id,
                    title,
                    type_str,
                    username,
                    raw_str,
                    formatear_fecha(msg_date),
                    folder_id,
                )
                procesados += 1

            if detener:
                break

            last_dialog = raw.dialogs[-1]
            last_msg_top = messages_map.get(last_dialog.top_message)
            offset_id = last_dialog.top_message
            offset_date = last_msg_top.date if last_msg_top else 0
            offset_peer = get_next_offset_peer(last_dialog.peer, users_map, chats_map)

        except Exception as e:
            print(f"❌ Error: {e}")
            break

    print(f"✅ Actualizados {procesados} chats en {nombre}.")


async def main():
    ensure_directories()
    async with client:
        print("🚀 Actualizador Diario Iniciado")
        await sincronizar_incremental(0)
        await sincronizar_incremental(1)


if __name__ == "__main__":
    asyncio.run(main())