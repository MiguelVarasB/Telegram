"""
Ruta principal (home).
"""
import aiosqlite
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pyrogram.raw.functions.messages import GetDialogFilters
from pyrogram.raw.types import DialogFilter, DialogFilterChatlist

from config import TEMPLATES_DIR, MAIN_TEMPLATE, DB_PATH
from services import get_client
from utils import obtener_id_limpio, formatear_miles

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/")
async def ver_home(request: Request):
    """Vista principal con lista de carpetas."""
    client = get_client()
    
    try:
        filtros = await client.invoke(GetDialogFilters())
    except Exception:
        filtros = []

    videos_total = 0
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM videos_telegram WHERE has_thumb > 0"
            ) as cursor:
                row = await cursor.fetchone()
                videos_total = int((row[0] if row else 0) or 0)
    except Exception:
        videos_total = 0
    
    chats_total = 0
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM chats") as cursor:
                row = await cursor.fetchone()
                chats_total = int((row[0] if row else 0) or 0)
    except Exception:
        chats_total = 0
    
    lista = [
        {"name": "Videos", "count": f"{formatear_miles(videos_total)} videos", "link": "/videos", "type": "system"},
        {"name": "Duplicados", "count": "Revisión", "link": "/duplicates", "type": "system"},
        {"name": "Inbox", "count": "General", "link": "/folder/0?name=Inbox", "type": "system"},
        {"name": "Archivados", "count": "Ocultos", "link": "/folder/1?name=Archivados", "type": "system"},
        {"name": "Todos los canales", "count": f"{formatear_miles(chats_total)} chats", "link": "/folder/-1?name=Todos%20los%20canales", "type": "system"},
    ]
    
    for f in filtros:
        if isinstance(f, (DialogFilter, DialogFilterChatlist)):
            nombre = getattr(f, 'title', 'Sin Título')
            include_p = getattr(f, 'include_peers', [])
            pinned_p = getattr(f, 'pinned_peers', [])
            
            # Usar set para evitar duplicados
            ids_unicos = set(obtener_id_limpio(p) for p in include_p + pinned_p if obtener_id_limpio(p))
            cnt = len(ids_unicos)
            lista.append({
                "name": nombre,
                "count": f"{formatear_miles(cnt)} chats",
                "link": f"/folder/{f.id}?name={nombre}",
                "type": "custom",
            })

    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": lista,
        "view_type": "root",
        "current_folder_name": "Inicio",
        "current_folder_url": "/",
        "current_channel_name": None,
        "parent_link": None,
    })
