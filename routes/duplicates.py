import os
import re
import sqlite3
import logging
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request, Body
from fastapi.templating import Jinja2Templates
from PIL import Image

# Importaciones de la base de datos
from database import get_db, transaction, DatabaseConnectionError

try:
    import torch
    from torchvision import transforms
    from torchvision.models import ResNet18_Weights, resnet18
except Exception:
    torch = None  # type: ignore
    transforms = None  # type: ignore
    ResNet18_Weights = None  # type: ignore
    resnet18 = None  # type: ignore

from config import CACHE_DUMP_VIDEOS_CHANNEL_ID, DB_PATH, MAIN_TEMPLATE, TEMPLATES_DIR, THUMB_FOLDER
from utils import convertir_tamano

router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)
logger = logging.getLogger(__name__)

_name_ext_re = re.compile(r"\.[a-z0-9]{1,6}$", re.IGNORECASE)
_name_clean_re = re.compile(r"[^a-z0-9 ]+", re.IGNORECASE)
_space_re = re.compile(r"\s+")
_SIM_MODEL = None
_SIM_TRANSFORM = None


def _normalize_name(name: str | None) -> str:
    raw = (name or "").strip()
    if not raw:
        return ""
    base = os.path.basename(raw)
    base = _name_ext_re.sub("", base)
    base = base.lower().replace("_", " ")
    base = _name_clean_re.sub(" ", base)
    base = _space_re.sub(" ", base).strip()
    return base


def _thumb_info(chat_id: int, video_id: str, mode: str) -> dict[str, Any]:
    thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
    if not os.path.exists(thumb_path):
        return {"exists": False, "bytes": None, "w": None, "h": None}

    info: dict[str, Any] = {"exists": True, "bytes": None, "w": None, "h": None}

    try:
        info["bytes"] = int(os.path.getsize(thumb_path) or 0)
    except Exception:
        info["bytes"] = None

    if mode == "wh":
        try:
            with Image.open(thumb_path) as img:
                w, h = img.size
                info["w"] = int(w)
                info["h"] = int(h)
        except Exception:
            info["w"] = None
            info["h"] = None

    return info


def _ensure_similarity_stack(device):
    if torch is None or transforms is None or resnet18 is None or ResNet18_Weights is None:
        raise HTTPException(
            status_code=503,
            detail="Se requiere torch+torchvision para el modo de semejanza (GPU).",
        )

    global _SIM_MODEL, _SIM_TRANSFORM
    if _SIM_MODEL is None:
        _SIM_MODEL = resnet18(weights=ResNet18_Weights.DEFAULT)
        _SIM_MODEL.fc = torch.nn.Identity()
        _SIM_MODEL.eval()
    _SIM_MODEL.to(device)

    if _SIM_TRANSFORM is None:
        _SIM_TRANSFORM = transforms.Compose(
            [
                transforms.Resize((224, 224)),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406],
                    std=[0.229, 0.224, 0.225],
                ),
            ]
        )

    return _SIM_MODEL, _SIM_TRANSFORM


def _embed_thumb_similarity(path: str, device) -> torch.Tensor | None:
    model, tfm = _ensure_similarity_stack(device)
    try:
        with Image.open(path) as img:
            img = img.convert("RGB")
            tensor = tfm(img).unsqueeze(0).to(device)
        with torch.no_grad():
            feat = model(tensor)
        return feat.squeeze(0)
    except Exception:
        return None


def _cluster_by_similarity(items: list[dict[str, Any]], similarity_threshold: float, device) -> dict[int, int]:
    if torch is None:
        raise HTTPException(
            status_code=503,
            detail="Se requiere torch para el modo de semejanza (GPU).",
        )

    embed_list: list[torch.Tensor] = []
    idx_list: list[int] = []

    for it in items:
        row_idx = it.get("row_idx")
        path = it.get("thumb_path")
        if not path or not os.path.exists(path):
            continue
        emb = _embed_thumb_similarity(path, device)
        if emb is None:
            continue
        embed_list.append(emb)
        idx_list.append(int(row_idx))

    if not embed_list:
        return {}

    feats = torch.stack(embed_list, dim=0).to(device)
    feats = feats / feats.norm(dim=1, keepdim=True).clamp_min(1e-8)
    sim = torch.matmul(feats, feats.t())

    pairs = (sim >= similarity_threshold).nonzero(as_tuple=False)

    parent = list(range(len(idx_list)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for p in pairs:
        i, j = int(p[0]), int(p[1])
        if i == j:
            continue
        union(i, j)

    root_to_id: dict[int, int] = {}
    clusters: dict[int, int] = {}
    next_id = 0
    for local_idx, original_idx in enumerate(idx_list):
        root = find(local_idx)
        if root not in root_to_id:
            root_to_id[root] = next_id
            next_id += 1
        clusters[original_idx] = root_to_id[root]

    return clusters


async def _ensure_thumb_bytes_column(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
        cols = [row[1] async for row in cursor]
    if "thumb_bytes" in cols:
        return
    try:
        await db.execute("ALTER TABLE videos_telegram ADD COLUMN thumb_bytes INTEGER;")
        await db.commit()
    except Exception:
        pass


async def _ensure_thumb_phash_column(db: aiosqlite.Connection) -> None:
    async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
        cols = [row[1] async for row in cursor]
    if "thumb_phash" in cols:
        return
    try:
        await db.execute("ALTER TABLE videos_telegram ADD COLUMN thumb_phash TEXT;")
        await db.commit()
    except Exception:
        pass


@router.get("/duplicates")
async def duplicates_page(request: Request):
    return templates.TemplateResponse(
        MAIN_TEMPLATE,
        {
            "request": request,
            "items": [],
            "view_type": "duplicates",
            "current_folder_name": "Duplicados",
            "current_folder_url": "/duplicates",
            "current_channel_name": None,
            "parent_link": "/",
            "dump_channel_id": CACHE_DUMP_VIDEOS_CHANNEL_ID,
        },
    )


@router.get("/api/duplicates")
async def api_duplicates(
    by_name: bool = Query(True),
    by_duration: bool = Query(True),
    by_video_size: bool = Query(True),
    by_thumb_size: bool = Query(False),
    by_thumb_phash: bool = Query(False),
    by_similarity: bool = Query(False),
    by_channel: bool = Query(True),
    duration_tol: int = Query(0, ge=0, le=60),
    size_tol_bytes: int = Query(0, ge=0, le=50_000_000),
    thumb_mode: str = Query("wh"),
    similarity_threshold: float = Query(0.92, ge=0.5, le=0.99),
    limit: int = Query(100, ge=1, le=200_000),
    min_group_size: int = Query(2, ge=2, le=50),
):
    if not any([by_name, by_duration, by_video_size, by_thumb_size, by_similarity, by_thumb_phash, by_channel]):
        by_name = True

    thumb_mode = thumb_mode if thumb_mode in ("wh", "bytes", "sim", "phash") else "wh"
    device = None
    if by_similarity:
        if torch is None:
            raise HTTPException(status_code=503, detail="Torch no disponible.")
        device = "cuda" if torch.cuda.is_available() else "cpu"

    # --- Filtros base SQL ---
    where_clauses = ["oculto = 0"]
    if by_thumb_size or by_similarity or by_thumb_phash:
        where_clauses.append("has_thumb > 0")
    where_sql = "WHERE " + " AND ".join(where_clauses)

    dur_bucket = max(1, duration_tol) if duration_tol > 0 else None
    size_bucket = max(1, size_tol_bytes) if size_tol_bytes > 0 else None

    # Expresiones de clave
    name_key_expr = "LOWER(COALESCE(nombre, ''))"
    channel_key_expr = "CAST(vt.chat_id AS INT)"
    dur_key_expr = (
        f"(CAST(COALESCE(duracion,0) AS INT)/{dur_bucket})*{dur_bucket}"
        if duration_tol > 0 else "CAST(COALESCE(duracion,0) AS INT)"
    )
    size_key_expr = (
        f"(CAST(COALESCE(tamano_bytes,0) AS INT)/{size_bucket})*{size_bucket}"
        if size_tol_bytes > 0 else "CAST(COALESCE(tamano_bytes,0) AS INT)"
    )

    # 1. Definimos mapeo de expresiones y sus alias para el CTE
    potential_keys = []
    if by_name: potential_keys.append((name_key_expr, "key_name"))
    if by_duration: potential_keys.append((dur_key_expr, "key_dur"))
    if by_video_size: potential_keys.append((size_key_expr, "key_size"))
    if by_channel: potential_keys.append((channel_key_expr, "key_channel"))

    # Construcción dinámica de SELECT, GROUP BY y JOIN
    cte_select_fields = ", ".join([f"{expr} AS {alias}" for expr, alias in potential_keys])
    cte_group_fields = ", ".join([alias for _, alias in potential_keys])
    join_conditions = " AND ".join([f"({expr} IS gc.{alias})" for expr, alias in potential_keys])

    query_sql = f"""
        WITH grupos_candidatos AS (
            SELECT {cte_select_fields}
            FROM videos_telegram vt
            {where_sql}
            GROUP BY {cte_group_fields}
            HAVING COUNT(*) >= ?
            ORDER BY COUNT(*) DESC
            LIMIT ? 
        )
        SELECT 
            vt.chat_id, vt.message_id, vt.nombre, vt.tamano_bytes, vt.duracion,
            vt.file_unique_id, vt.file_id, vt.has_thumb, vt.fecha_mensaje,
            vt.thumb_bytes, vt.thumb_phash,
            c.name AS chat_name, c.username AS chat_username,
            {name_key_expr if by_name else 'NULL'} AS key_name,
            {dur_key_expr if by_duration else 'NULL'} AS key_dur,
            {size_key_expr if by_video_size else 'NULL'} AS key_size
        FROM videos_telegram vt
        JOIN grupos_candidatos gc ON {join_conditions}
        LEFT JOIN chats c ON c.chat_id = vt.chat_id
        {where_sql}
        ORDER BY vt.tamano_bytes DESC, vt.fecha_mensaje DESC
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await _ensure_thumb_bytes_column(db)
        await _ensure_thumb_phash_column(db)
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute(query_sql, (min_group_size, limit)) as cursor:
                rows = await cursor.fetchall()
        except sqlite3.OperationalError as e:
            logger.error(f"Error SQL: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    rows = rows or []
    
    # --- Procesamiento de Mensajes Asociados ---
    video_ids = {(r["file_unique_id"] or "").strip() for r in rows if r["file_unique_id"]}
    msg_counts: dict[str, int] = {}
    if video_ids:
        placeholders = ",".join("?" for _ in video_ids)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(f"SELECT video_id, COUNT(*) FROM video_messages WHERE video_id IN ({placeholders}) GROUP BY video_id", tuple(video_ids)) as cursor:
                async for row in cursor:
                    msg_counts[row[0].strip()] = row[1]

    # --- Semejanza (GPU/CPU) ---
    similarity_clusters = {}
    if by_similarity and rows:
        sim_items = []
        for idx, r in enumerate(rows):
            path = os.path.join(THUMB_FOLDER, str(r["chat_id"]), f"{r['file_unique_id']}.webp")
            if os.path.exists(path):
                sim_items.append({"row_idx": idx, "thumb_path": path})
        if sim_items:
            similarity_clusters = _cluster_by_similarity(sim_items, similarity_threshold, device)

    # --- Agrupamiento Final ---
    groups: dict[tuple, dict] = {}
    for idx, r in enumerate(rows):
        chat_id = int(r["chat_id"])
        video_id = (r["file_unique_id"] or "").strip()
        
        # Generar clave de grupo
        key_parts = []
        key_view = {}
        
        if by_name:
            key_parts.append(r["key_name"])
            key_view["name"] = r["key_name"]
        if by_duration:
            key_parts.append(r["key_dur"])
            key_view["duration"] = r["key_dur"]
        if by_video_size:
            key_parts.append(r["key_size"])
            key_view["video_size"] = r["key_size"]
        if by_channel:
            key_parts.append(chat_id)
            key_view["channel"] = chat_id
            
        # Filtros extra de Python (Phash / Semejanza)
        if by_thumb_phash:
            phash = (r["thumb_phash"] or "").strip()
            key_parts.append(phash or f"no_phash_{idx}")
            key_view["phash"] = phash
        if by_similarity:
            sim_id = similarity_clusters.get(idx)
            key_parts.append(sim_id if sim_id is not None else f"no_sim_{idx}")
            key_view["sim_cluster"] = sim_id

        group_key = tuple(key_parts)
        if group_key not in groups:
            groups[group_key] = {"key": key_view, "items": []}

        item = {
            "chat_id": chat_id,
            "message_id": r["message_id"],
            "video_id": video_id,
            "file_id": r["file_id"],
            "messages_count": msg_counts.get(video_id, 0),
            "nombre": r["nombre"] or f"Video {r['message_id']}",
            "duracion": r["duracion"],
            "tamano_bytes": r["tamano_bytes"],
            "tamano_text": convertir_tamano(r["tamano_bytes"]),
            "fecha_mensaje": r["fecha_mensaje"],
            "has_thumb": r["has_thumb"],
            "chat_name": r["chat_name"],
            "chat_username": r["chat_username"],
            "play_link": f"/play/{chat_id}/{r['message_id']}",
            "thumb_url": f"/api/photo/{r['file_id']}?chat_id={chat_id}&video_id={video_id}" if r["file_id"] else None,
        }
        groups[group_key]["items"].append(item)

    # Ordenar items dentro de cada grupo por cantidad de mensajes y fecha
    for g in groups.values():
        def _key(it: dict[str, Any]):
            try:
                msgs = int(it.get("messages_count") or 0)
            except Exception:
                msgs = 0
            try:
                fecha = int(it.get("fecha_mensaje") or 0)
            except Exception:
                fecha = 0
            try:
                mid = int(it.get("message_id") or 0)
            except Exception:
                mid = 0
            return (msgs, fecha, mid)

        g["items"].sort(key=_key, reverse=True)

    # Filtrar por tamaño mínimo de grupo y ordenarlos
    out_groups = [g for g in groups.values() if len(g["items"]) >= min_group_size]
    for g in out_groups:
        msg_counts_group = [int(it.get("messages_count") or 0) for it in g["items"]]
        fechas_group = [it.get("fecha_mensaje") or 0 for it in g["items"]]
        mids_group = [int(it.get("message_id") or 0) for it in g["items"]]
        g["total_msgs"] = sum(msg_counts_group)
        g["max_msgs"] = max(msg_counts_group) if msg_counts_group else 0
        g["latest_fecha"] = max(fechas_group) if fechas_group else 0
        g["latest_mid"] = max(mids_group) if mids_group else 0

    out_groups.sort(
        key=lambda g: (
            g.get("max_msgs", 0),
            g.get("total_msgs", 0),
            len(g["items"]),
            g.get("latest_fecha", 0),
            g.get("latest_mid", 0),
        ),
        reverse=True,
    )

    return {
        "scanned": len(rows),
        "groups": out_groups,
        "groups_count": len(out_groups)
    }


@router.post("/api/duplicates/hide")
async def hide_duplicates(payload: dict = Body(...)):
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Items inválidos.")

    pairs = [(int(it["chat_id"]), int(it["message_id"])) for it in items if "chat_id" in it and "message_id" in it]
    
    try:
        async with get_db() as db:
            async with transaction(db) as cursor:
                await cursor.executemany("UPDATE videos_telegram SET oculto = 2 WHERE chat_id = ? AND message_id = ?", pairs)
        return {"ok": True, "updated": len(pairs)}
    except Exception as e:
        logger.error(f"Error hide: {e}")
        raise HTTPException(status_code=500, detail=str(e))