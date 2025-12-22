"""
Rutas de carpetas (folders).
"""
import os
import json
import asyncio
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pyrogram.raw.functions.messages import GetDialogs
from pyrogram.raw.types import InputPeerEmpty, PeerUser, PeerChat, PeerChannel

from config import TEMPLATES_DIR, JSON_FOLDER, MAIN_TEMPLATE
from services import get_client, refresh_manual_folder_from_telegram
from database import db_add_chat_folder, get_folder_items_from_db
from utils import serialize_pyrogram, json_serial, formatear_miles
from utils.websocket import ws_manager

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/folder/{folder_id}")
async def ver_carpeta(request: Request, folder_id: int, name: str = "Carpeta"):
    """Vista de carpeta con lista de chats."""
    client = get_client()
    lista_chats = []
    raw_dump = []
    
    # A. CARPETAS DE SISTEMA (Inbox=0, Archivados=1)
    if folder_id in [0, 1]:
        lista_chats = await get_folder_items_from_db(folder_id, name)
        raw_dump = []

        # Si la BD no tiene datos, consultar Telegram (Fallback)
        if not lista_chats:
            try:
                raw = await client.invoke(GetDialogs(
                    offset_date=0, offset_id=0, offset_peer=InputPeerEmpty(),
                    limit=1000, hash=0, folder_id=folder_id
                ))
                
                chats_map = {c.id: c for c in raw.chats}
                users_map = {u.id: u for u in raw.users}
                
                for d in raw.dialogs:
                    p = d.peer
                    entity, final_id = None, 0
                    
                    if isinstance(p, PeerUser):
                        entity, final_id = users_map.get(p.user_id), p.user_id
                    elif isinstance(p, PeerChat):
                        entity, final_id = chats_map.get(p.chat_id), int(f"-{p.chat_id}")
                    elif isinstance(p, PeerChannel):
                        entity, final_id = chats_map.get(p.channel_id), int(f"-100{p.channel_id}")
                    
                    disp = getattr(entity, 'title', None) if entity else "Desconocido"
                    if not disp and entity:
                        disp = f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()

                    username = getattr(entity, 'username', None) if entity else None
                    telegram_link = f"https://t.me/{username}" if username else None
                    
                    info_text = f"{formatear_miles(d.unread_count)} sin leer"
                    
                    lista_chats.append({
                        "name": disp,
                        "count": info_text,
                        "link": f"/channel/{final_id}?name={disp}&folder_id={folder_id}&folder_name={name}",
                        "type": "chat",
                        "photo_id": None,
                        "folder_id": folder_id,
                        "telegram_link": telegram_link,
                    })
                    # --- CAMBIO: AWAIT AQUI ---
                    await db_add_chat_folder(final_id, folder_id)
            except Exception as e:
                print(f"Err sys: {e}")
    
    # B. CARPETAS MANUALES
    else:
        lista_chats = await get_folder_items_from_db(folder_id, name)
        raw_dump = []
        asyncio.create_task(refresh_manual_folder_from_telegram(folder_id, name))

    dump_data_folder = {
        "folder_id": folder_id,
        "name": name,
        "items": lista_chats,
        "raw": raw_dump,
    }
    dump_path = os.path.join(JSON_FOLDER, f"folder_dump_{folder_id}.json")
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump(dump_data_folder, f, indent=4, default=json_serial, ensure_ascii=False)

    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": lista_chats,
        "view_type": "folder",
        "current_folder_name": name,
        "current_folder_id": folder_id,
        "current_folder_url": f"/folder/{folder_id}?name={name}",
        "current_channel_name": None,
        "parent_link": "/",
    })


@router.websocket("/ws/folder/{folder_id}")
async def folder_ws(websocket: WebSocket, folder_id: int):
    """WebSocket para notificaciones de refresco de carpeta."""
    await ws_manager.connect(folder_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(folder_id, websocket)


@router.get("/api/folder/{folder_id}")
async def api_folder(folder_id: int):
    """API JSON para obtener items de una carpeta."""
    items = await get_folder_items_from_db(folder_id)
    return JSONResponse(items)