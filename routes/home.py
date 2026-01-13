"""
Ruta principal (home) - Optimizada con cache.
"""
import aiosqlite
import asyncio
from datetime import datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pyrogram.raw.functions.messages import GetDialogFilters
from pyrogram.raw.types import DialogFilter, DialogFilterChatlist

from config import TEMPLATES_DIR, MAIN_TEMPLATE, DB_PATH
from database.connection import get_db
from services import get_client
from utils import obtener_id_limpio, formatear_miles, log_timing

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Cache para conteos (actualizado cada 5 minutos)
_cache_videos = {"count": 0, "updated": None}
_cache_chats = {"count": 0, "updated": None}
_cache_filters = {"filters": [], "updated": None}
CACHE_DURATION = timedelta(minutes=5)


async def _get_cached_videos_count():
    """Obtener conteo de videos con cache de 5 minutos."""
    now = datetime.now()
    
    # Si cache es válido, retornar valor cacheado
    if (_cache_videos["updated"] and 
        now - _cache_videos["updated"] < CACHE_DURATION):
        return _cache_videos["count"]
    
    # Actualizar cache
    try:
        async with get_db() as db:
            async with db.execute(
                "SELECT COUNT(*) FROM videos_telegram WHERE has_thumb > 0"
            ) as cursor:
                row = await cursor.fetchone()
                count = int((row[0] if row else 0) or 0)
        
        _cache_videos["count"] = count
        _cache_videos["updated"] = now
        return count
    except Exception:
        return _cache_videos["count"] or 0


async def _get_cached_chats_count():
    """Obtener conteo de chats con cache de 5 minutos."""
    now = datetime.now()
    
    # Si cache es válido, retornar valor cacheado
    if (_cache_chats["updated"] and 
        now - _cache_chats["updated"] < CACHE_DURATION):
        return _cache_chats["count"]
    
    # Actualizar cache
    try:
        async with get_db() as db:
            async with db.execute("SELECT COUNT(*) FROM chats") as cursor:
                row = await cursor.fetchone()
                count = int((row[0] if row else 0) or 0)
        
        _cache_chats["count"] = count
        _cache_chats["updated"] = now
        return count
    except Exception:
        return _cache_chats["count"] or 0


async def _get_cached_filters():
    """Obtener filtros de Telegram con cache de 5 minutos."""
    now = datetime.now()
    
    # Si cache es válido, retornar valor cacheado
    if (_cache_filters["updated"] and 
        now - _cache_filters["updated"] < CACHE_DURATION):
        return _cache_filters["filters"]
    
    # Actualizar cache
    try:
        client = get_client()
        filters = await client.invoke(GetDialogFilters())
        
        _cache_filters["filters"] = filters
        _cache_filters["updated"] = now
        return filters
    except Exception:
        return _cache_filters["filters"] or []


@router.get("/")
async def ver_home(request: Request):
    """Vista principal con lista de carpetas - Optimizada con cache."""
    log_timing(" Iniciando endpoint /..")
    
    # Ejecutar todas las operaciones en paralelo
    log_timing(" Obteniendo datos cacheados/en paralelo..")
    
    # Crear tareas para ejecución paralela
    tasks = [
        _get_cached_filters(),
        _get_cached_videos_count(), 
        _get_cached_chats_count()
    ]
    
    # Esperar todas las tareas en paralelo
    filtros, videos_total, chats_total = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Manejar excepciones
    if isinstance(filtros, Exception):
        filtros = []
    if isinstance(videos_total, Exception):
        videos_total = 0
    if isinstance(chats_total, Exception):
        chats_total = 0
    
    log_timing(f" Datos obtenidos: {len(filtros)} filtros, {videos_total} videos, {chats_total} chats")
    
    # Construir lista base
    lista = [
        {"name": "Videos", "count": f"{formatear_miles(videos_total)} videos", "link": "/videos", "type": "system"},
        {"name": "Duplicados", "count": "Revisión", "link": "/duplicates", "type": "system"},
        {"name": "Inbox", "count": "General", "link": "/folder/0?name=Inbox", "type": "system"},
        {"name": "Archivados", "count": "Ocultos", "link": "/folder/1?name=Archivados", "type": "system"},
        {"name": "Todos los canales", "count": f"{formatear_miles(chats_total)} chats", "link": "/folder/-1?name=Todos%20los%20canales", "type": "system"},
    ]
    
    # Procesar filtros personalizados
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
    
    log_timing("Endpoint / terminado")
    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": lista,
        "view_type": "root",
        "current_folder_name": "Inicio",
        "current_folder_url": "/",
        "current_channel_name": None,
        "parent_link": None,
    })
