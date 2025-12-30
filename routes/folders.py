"""
Rutas de carpetas (folders).
"""
import os
import json
import asyncio
import uuid
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks, Body

from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from pyrogram.raw.functions.messages import GetDialogs
from pyrogram.raw.types import InputPeerEmpty, PeerUser, PeerChat, PeerChannel

from config import TEMPLATES_DIR, JSON_FOLDER, MAIN_TEMPLATE
from services import get_client, refresh_manual_folder_from_telegram
from database import db_add_chat_folder, get_folder_items_from_db
from database.folders import get_all_chats_with_counts
from utils import serialize_pyrogram, json_serial, log_timing
from utils.websocket import ws_manager
from .channels import scan_channel_background

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Máximo 2 escaneos concurrentes para evitar bloqueos de SQLite
SCAN_CONCURRENCY = 2
SCAN_SEMAPHORE = asyncio.Semaphore(SCAN_CONCURRENCY)


def _parse_limite_videos(request: Request, default: int = 100) -> int:
    val = request.query_params.get("limite") or request.query_params.get("limit")
    try:
        if val is None:
            return default
        return max(1, int(val))
    except Exception:
        return default


def _parse_sort_params(request: Request):
    """
    Lee parámetros de ordenado permitidos.
    sort: faltantes, indexados, totales, nombre, fecha_scan, ultimo_msg, completos
    direction: asc | desc
    """
    allowed_sorts = {
        "faltantes",
        "indexados",
        "totales",
        "nombre",
        "fecha_scan",
        "ultimo_msg",
        "completos",
    }
    sort = request.query_params.get("sort", "").lower()
    direction = request.query_params.get("direction", "").lower()
    if sort not in allowed_sorts:
        sort = "faltantes"
    if direction not in {"asc", "desc"}:
        direction = "desc"
    return sort, direction


@router.get("/folder/{folder_id}")
async def ver_carpeta(request: Request, folder_id: int, name: str = "Carpeta"):
    """Vista de carpeta con lista de chats.

    Nota: Los datos reales se cargan vía WebSocket para mejor rendimiento.
    Esta función solo devuelve el template HTML inicial.
    """
    sort, direction = _parse_sort_params(request)

    # No cargamos datos aquí; los items se llenan vía WebSocket
    return templates.TemplateResponse(MAIN_TEMPLATE, {
        "request": request,
        "items": [],
        "view_type": "folder",
        "current_folder_name": name,
        "current_folder_id": folder_id,
        "current_folder_url": f"/folder/{folder_id}?name={name}",
        "current_sort": sort,
        "current_direction": direction,
        "current_channel_name": None,
        "parent_link": "/",
    })


@router.websocket("/ws/folder/{folder_id}")
async def folder_ws(websocket: WebSocket, folder_id: int):
    """WebSocket para notificaciones de refresco de carpeta."""
    limite_videos_default = 999888777
    await ws_manager.connect(folder_id, websocket)
    # Enviar datos iniciales en segundo plano para no bloquear la conexión
    async def send_initial_items():
        try:
            # Parámetros desde la URL del WS
            val_limit = websocket.query_params.get("limite") or websocket.query_params.get("limit")
            try:
                limite_videos = max(1, int(val_limit)) if val_limit is not None else limite_videos_default
            except Exception:
                limite_videos = limite_videos_default

            sort = websocket.query_params.get("sort", "faltantes").lower()
            direction = websocket.query_params.get("direction", "desc").lower()
  
            if folder_id == -1:
                
                items = await get_all_chats_with_counts(
                    "Todos los canales",
                    limite_videos,
                    sort_field=sort,
                    direction=direction,
                )
            elif folder_id in [0, 1]:
                items = await get_folder_items_from_db(folder_id, "Carpeta")
            else:
                items = await get_folder_items_from_db(folder_id, "Carpeta")
            await websocket.send_json({
                "type": "init",
                "items": items,
                "sort": sort,
                "direction": direction,
            })
        except Exception as e:
            try:
                await websocket.send_json({"type": "error", "message": str(e)})
            except Exception:
                pass

    asyncio.create_task(send_initial_items())

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


# ---- Escaneo en lote con control de concurrencia y progreso ----

async def _broadcast_progress(payload: dict, folder_id: int | None):
    """Envía un mensaje de progreso, filtrado por carpeta si se pasa."""
    try:
        await ws_manager.broadcast_event(payload, folder_id=folder_id)
    except Exception:
        pass


async def _scan_single(chat_id: int, job_id: str, folder_id: int | None, counters: dict):
    await _broadcast_progress({
        "type": "batch_scan_update",
        "job_id": job_id,
        "chat_id": chat_id,
        "status": "started",
        "done": counters["done"],
        "total": counters["total"],
        "running": counters["running"] + 1,
    }, folder_id)

    async with SCAN_SEMAPHORE:
        try:
            counters["running"] += 1
            await scan_channel_background(chat_id, run_thumb_worker=False)
            status = "finished"
            error = None
        except Exception as e:
            status = "error"
            error = str(e)
        finally:
            counters["running"] -= 1
            counters["done"] += 1

    await _broadcast_progress({
        "type": "batch_scan_update",
        "job_id": job_id,
        "chat_id": chat_id,
        "status": status,
        "error": error,
        "done": counters["done"],
        "total": counters["total"],
        "running": counters["running"],
    }, folder_id)


async def _run_batch_scan(chat_ids: list[int], folder_id: int | None, job_id: str):
    counters = {"done": 0, "total": len(chat_ids), "running": 0}
    await _broadcast_progress({
        "type": "batch_scan_start",
        "job_id": job_id,
        "total": counters["total"],
        "concurrency": SCAN_CONCURRENCY,
    }, folder_id)

    tasks = [asyncio.create_task(_scan_single(cid, job_id, folder_id, counters)) for cid in chat_ids]
    await asyncio.gather(*tasks)

    await _broadcast_progress({
        "type": "batch_scan_done",
        "job_id": job_id,
        "total": counters["total"],
        "done": counters["done"],
    }, folder_id)


@router.post("/api/folder/scan-batch")
async def api_folder_scan_batch(
    body: dict = Body(..., example={"chat_ids": [123, 456], "folder_id": 1, "job_id": "opcional"}),
):
    """
    Recibe una lista de chat_ids a escanear. El backend controla la concurrencia (2 a la vez)
    y emite progreso por WebSocket con los tipos:
      - batch_scan_start
      - batch_scan_update
      - batch_scan_done
    """
    chat_ids = body.get("chat_ids") or []
    if not isinstance(chat_ids, list) or not chat_ids:
        raise HTTPException(status_code=400, detail="chat_ids debe ser una lista no vacía")

    folder_id = body.get("folder_id")
    job_id = body.get("job_id") or str(uuid.uuid4())

    # Lanzamos tarea en background (sin bloquear la respuesta HTTP)
    asyncio.create_task(_run_batch_scan(chat_ids, folder_id, job_id))

    return {
        "status": "scheduled",
        "job_id": job_id,
        "total": len(chat_ids),
        "concurrency": SCAN_CONCURRENCY,
    }