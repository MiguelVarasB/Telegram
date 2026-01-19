"""
Rutas para visualizar videos dentro de un canal.
Ahora incluye disparador de actualización de estadísticas en segundo plano.
"""
import math
from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from config import TEMPLATES_DIR
from database import db_get_channel_videos, db_get_chat_info
from database.counters import update_chat_stats_background # <--- IMPORTANTE

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

@router.get("/channel/{chat_id}", response_class=HTMLResponse)
async def page_channel(
    request: Request, 
    chat_id: int, 
    page: int = 1,
    q: str = "",
    sort: str = "date_desc", # date_desc, date_asc, size_desc, size_asc, duration_desc
    filter_type: str = "all" # all, no_thumb, vertical, long
):
    """
    Muestra los videos de un canal con paginación y filtros.
    """
    limit = 50
    offset = (page - 1) * limit

    # 1. Obtener información del chat
    chat_info = await db_get_chat_info(chat_id)
    chat_name = chat_info["title"] if chat_info else f"Chat {chat_id}"
    
    # 2. Obtener videos (usando tu función existente de base de datos)
    videos, total_count = await db_get_channel_videos(
        chat_id=chat_id,
        limit=limit,
        offset=offset,
        search_query=q,
        sort_order=sort,
        filter_type=filter_type
    )

    # 3. 🔥 DISPARADOR MÁGICO (BACKGROUND)
    # Esto actualiza la tabla de estadísticas para este chat específico.
    # Al ser background task, NO retrasa la carga de la página.
    await update_chat_stats_background(chat_id)

    # 4. Cálculos de paginación
    total_pages = math.ceil(total_count / limit) if limit > 0 else 1
    pagination_start = max(1, page - 2)
    pagination_end = min(total_pages, page + 2)
    
    return templates.TemplateResponse("partials/content.html", {
        "request": request,
        "page_type": "channel_videos",
        "items": videos,
        "chat_id": chat_id,
        "chat_name": chat_name,
        "current_page": page,
        "total_pages": total_pages,
        "total_items": total_count,
        "query": q,
        "sort": sort,
        "filter_type": filter_type,
        "pagination_start": pagination_start,
        "pagination_end": pagination_end
    })