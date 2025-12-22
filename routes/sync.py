"""
Endpoint para ejecutar la sincronización diaria (similar a sincronizar_diario.py).
"""
import datetime
import time
import traceback
import aiosqlite
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pyrogram.raw.functions.messages import GetDialogs
from pyrogram.raw.types import (
    InputPeerEmpty,
    InputPeerUser,
    InputPeerChannel,
    InputPeerChat,
    PeerUser,
    PeerChat,
    PeerChannel,
)

from config import DB_PATH
from services import get_client

router = APIRouter()

# Fecha de corte (24h)
HORAS_ATRAS = 24


def formatear_fecha(timestamp: float | None):
    if not timestamp:
        return None
    return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")


async def guardar_en_bd(db: aiosqlite.Connection, chat_id, name, type_str, username, raw_json, last_date, folder_id):
    await db.execute(
        """
        INSERT INTO chats (chat_id, name, type, username, raw_json, updated_at, last_message_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            name=excluded.name,
            username=excluded.username,
            updated_at=excluded.updated_at,
            last_message_date=excluded.last_message_date
        """,
        (chat_id, name, type_str, username, raw_json, datetime.datetime.now().isoformat(), last_date),
    )
    await db.execute(
        "INSERT OR IGNORE INTO chat_folders (chat_id, folder_id) VALUES (?, ?)",
        (chat_id, folder_id),
    )


def get_next_offset_peer(peer, users_map, chats_map):
    try:
        if isinstance(peer, PeerUser):
            user = users_map.get(peer.user_id)
            if user:
                return InputPeerUser(user_id=user.id, access_hash=user.access_hash)
        elif isinstance(peer, PeerChannel):
            channel = chats_map.get(peer.channel_id)
            if channel:
                return InputPeerChannel(channel_id=channel.id, access_hash=channel.access_hash)
        elif isinstance(peer, PeerChat):
            return InputPeerChat(chat_id=peer.chat_id)
    except Exception:
        pass
    return InputPeerEmpty()


async def sincronizar_incremental(folder_id: int) -> dict:
    client = get_client()
    nombre = "INBOX (0)" if folder_id == 0 else "ARCHIVADOS (1)"
    print(f"🔄 Escaneando cambios recientes en {nombre}...")

    offset_date = 0
    offset_id = 0
    offset_peer = InputPeerEmpty()
    limit = 100
    procesados = 0
    detener = False

    timestamp_limite = time.time() - (HORAS_ATRAS * 3600)

    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous = NORMAL")
        await db.execute("PRAGMA busy_timeout = 5000")

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

                writes_in_batch = 0

                for d in raw.dialogs:
                    top_msg = messages_map.get(d.top_message)
                    msg_date = top_msg.date if top_msg else 0
                    is_pinned = getattr(d, "pinned", False)

                    if not is_pinned and msg_date < timestamp_limite:
                        print(f"   🛑 Encontrado chat antiguo ({formatear_fecha(msg_date)}). Deteniendo escaneo.")
                        detener = True
                        break

                    if msg_date < timestamp_limite:
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

                    await guardar_en_bd(db, chat_id, title, type_str, username, raw_str, formatear_fecha(msg_date), folder_id)
                    procesados += 1
                    writes_in_batch += 1

                if writes_in_batch:
                    await db.commit()

                if detener:
                    break

                last_dialog = raw.dialogs[-1]
                last_msg_top = messages_map.get(last_dialog.top_message)
                offset_id = last_dialog.top_message
                offset_date = last_msg_top.date if last_msg_top else 0
                offset_peer = get_next_offset_peer(last_dialog.peer, users_map, chats_map)

            except Exception as e:
                print(f"❌ Error ({type(e).__name__}): {e}")
                traceback.print_exc()
                break

    print(f"✅ Actualizados {procesados} chats en {nombre}.")
    return {"folder_id": folder_id, "nombre": nombre, "procesados": procesados}


@router.post("/sync/diario")
async def sync_diario():
    """
    Ejecuta la sincronización incremental para Inbox (0) y Archivados (1).
    """
    resultados = []
    for fid in (0, 1):
        res = await sincronizar_incremental(fid)
        resultados.append(res)
    return JSONResponse({"status": "ok", "resultados": resultados})
