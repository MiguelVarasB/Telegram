import os
import re
import sqlite3
import logging
from typing import Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi import Body
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
    """Garantiza que exista la columna thumb_bytes; intenta en la misma conexión."""
    async with db.execute("PRAGMA table_info(videos_telegram)") as cursor:
        cols = [row[1] async for row in cursor]
    if "thumb_bytes" in cols:
        return
    try:
        await db.execute("ALTER TABLE videos_telegram ADD COLUMN thumb_bytes INTEGER;")
        await db.commit()
    except Exception:
        # si ya existe o falla, ignoramos; se validará de nuevo en table_info
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
    by_similarity: bool = Query(False),
    duration_tol: int = Query(0, ge=0, le=60),
    size_tol_bytes: int = Query(0, ge=0, le=50_000_000),
    thumb_mode: str = Query("wh"),
    similarity_threshold: float = Query(0.92, ge=0.5, le=0.99),
    limit: int = Query(50_000, ge=1, le=200_000),
    min_group_size: int = Query(2, ge=2, le=50),
):
    if not (by_name or by_duration or by_video_size or by_thumb_size or by_similarity):
        by_name = True

    thumb_mode = thumb_mode if thumb_mode in ("wh", "bytes") else "wh"
    device = None
    if by_similarity:
        if torch is None:
            raise HTTPException(
                status_code=503,
                detail="Torch/torchvision no están disponibles para semejanza.",
            )
        device = "cuda" if torch.cuda.is_available() else "cpu"

    # --- Filtros “duros” en SQL (etapa 1)
    where_clauses = ["oculto = 0"]
    if by_thumb_size or by_similarity:
        where_clauses.append("has_thumb = 1")
    where_sql = "WHERE " + " AND ".join(where_clauses)

    dur_bucket = max(1, duration_tol) if duration_tol > 0 else None
    size_bucket = max(1, size_tol_bytes) if size_tol_bytes > 0 else None

    name_key_expr = "LOWER(COALESCE(nombre, ''))" if by_name else "NULL"
    dur_key_expr = (
        f"(CAST(COALESCE(duracion,0) AS INT)/{dur_bucket})*{dur_bucket}"
        if by_duration and dur_bucket
        else "CAST(COALESCE(duracion,0) AS INT)" if by_duration else "NULL"
    )
    size_key_expr = (
        f"(CAST(COALESCE(tamano_bytes,0) AS INT)/{size_bucket})*{size_bucket}"
        if by_video_size and size_bucket
        else "CAST(COALESCE(tamano_bytes,0) AS INT)" if by_video_size else "NULL"
    )

    partition_fields = [
        expr for expr in (by_name and name_key_expr, by_duration and dur_key_expr, by_video_size and size_key_expr) if expr
    ]
    window_count = (
        f"COUNT(*) OVER (PARTITION BY {', '.join(partition_fields)})"
        if partition_fields
        else "COUNT(*) OVER ()"
    )

    query_sql = f"""
        WITH base AS (
            SELECT
                chat_id, message_id, nombre, tamano_bytes, duracion,
                file_unique_id, file_id, has_thumb, fecha_mensaje, thumb_bytes,
                {name_key_expr} AS key_name,
                {dur_key_expr} AS key_dur,
                {size_key_expr} AS key_size,
                {window_count} AS grp_count
            FROM videos_telegram
            {where_sql}
        )
        SELECT chat_id, message_id, nombre, tamano_bytes, duracion,
               file_unique_id, file_id, has_thumb, fecha_mensaje,
               thumb_bytes,
               key_name, key_dur, key_size
        FROM base
        WHERE grp_count >= ?
        ORDER BY grp_count DESC, fecha_mensaje DESC, message_id DESC
        LIMIT ?
    """
    print (query_sql)
    print (f"Limite: {limit}")
    async with aiosqlite.connect(DB_PATH) as db:
        await _ensure_thumb_bytes_column(db)
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute(
                query_sql,
                (min_group_size, limit),
            ) as cursor:
                rows = await cursor.fetchall()
               
        except sqlite3.OperationalError as e:
            if "thumb_bytes" in str(e):
                await _ensure_thumb_bytes_column(db)
                async with db.execute(
                    query_sql,
                    (min_group_size, limit),
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                raise

    rows = rows or []
    print (f"cantidad: {len(rows)}")
    # Conteo de mensajes asociados por video_id (para mostrar en la tarjeta)
    video_ids = {
        (r["file_unique_id"] or "").strip()
        for r in rows
        if (r["file_unique_id"] or "").strip()
    }
    msg_counts: dict[str, int] = {}
    if video_ids:
        placeholders = ",".join("?" for _ in video_ids)
        sql = f"""
            SELECT video_id, COUNT(*) AS total
            FROM video_messages
            WHERE video_id IN ({placeholders})
            GROUP BY video_id
        """
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(sql, tuple(video_ids)) as cursor:
                async for row in cursor:
                    vid = (row[0] or "").strip()
                    msg_counts[vid] = int(row[1] or 0)

    similarity_clusters: dict[int, int] = {}
    if by_similarity:
        sim_items: list[dict[str, Any]] = []
        for row_idx, r in enumerate(rows):
            chat_id = int(r["chat_id"])
            video_id = (r["file_unique_id"] or "").strip()
            thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
            if not os.path.exists(thumb_path):
                continue
            sim_items.append(
                {
                    "row_idx": row_idx,
                    "thumb_path": thumb_path,
                }
            )

        if sim_items:
            similarity_clusters = _cluster_by_similarity(sim_items, similarity_threshold, device)

    groups: dict[tuple[Any, ...], dict[str, Any]] = {}

    for row_idx, r in enumerate(rows):
        chat_id = int(r["chat_id"])
        message_id = int(r["message_id"])
        file_unique_id=(r["file_unique_id"])
        video_id = (r["file_unique_id"] or "").strip()
        file_id = (r["file_id"] or "").strip()
        nombre = r["nombre"]

        dur_raw = r["duracion"]
        try:
            dur = int(round(float(dur_raw or 0)))
        except Exception:
            dur = 0

        size_raw = r["tamano_bytes"]
        try:
            size_bytes = int(size_raw or 0)
        except Exception:
            size_bytes = 0

        # Claves calculadas en SQL (etapa 1). Si no se usó la clave, calculamos aquí.
        name_norm = _normalize_name(nombre) if by_name else None
        name_key: Any = r["key_name"] if by_name else None
        if by_name and not name_key:
            name_key = ("empty", chat_id, message_id)

        dur_key: int | None = None
        if by_duration:
            if r["key_dur"] is not None:
                dur_key = int(r["key_dur"])
            elif duration_tol and duration_tol > 0:
                bucket = max(1, duration_tol)
                dur_key = int((dur // bucket) * bucket)
            else:
                dur_key = dur

        size_key: int | None = None
        if by_video_size:
            if r["key_size"] is not None:
                size_key = int(r["key_size"])
            elif size_tol_bytes and size_tol_bytes > 0:
                bucket = max(1, size_tol_bytes)
                size_key = int((size_bytes // bucket) * bucket)
            else:
                size_key = size_bytes

        thumb_key: Any = None
        thumb_info: dict[str, Any] | None = None
        if by_thumb_size:
            if thumb_mode == "bytes":
                thumb_bytes = r["thumb_bytes"] if "thumb_bytes" in r.keys() else None
                thumb_key = thumb_bytes
                thumb_info = {"exists": thumb_bytes is not None, "bytes": thumb_bytes, "w": None, "h": None}
            else:
                thumb_info = _thumb_info(chat_id, video_id, thumb_mode)
                if not thumb_info.get("exists"):
                    thumb_key = ("missing", chat_id, message_id)
                else:
                    thumb_key = (thumb_info.get("w"), thumb_info.get("h"))

        sim_key: Any = None
        if by_similarity:
            sim_key = similarity_clusters.get(row_idx)

        key_parts: list[Any] = []
        key_view: dict[str, Any] = {}

        if by_name:
            key_parts.append(name_key)
            key_view["name"] = name_norm
        if by_duration:
            key_parts.append(dur_key)
            key_view["duration"] = dur_key
        if by_video_size:
            key_parts.append(size_key)
            key_view["video_size"] = size_key
        if by_thumb_size:
            key_parts.append(thumb_key)
            key_view["thumb"] = thumb_key
        if by_similarity:
            key_parts.append(sim_key)
            key_view["similarity_cluster"] = sim_key
            key_view["threshold"] = similarity_threshold

        key = tuple(key_parts)

        if key not in groups:
            groups[key] = {"key": key_view, "items": []}

        item = {
            "chat_id": chat_id,
            "message_id": message_id,
            "video_id": video_id,
            "file_unique_id":file_unique_id,
            "file_id": file_id,
            "messages_count": msg_counts.get(video_id, 0),
            "nombre": nombre or f"Video {message_id}",
            "nombre_norm": name_norm,
            "duracion": dur,
            "tamano_bytes": size_bytes,
            "tamano_text": convertir_tamano(size_bytes),
            "fecha_mensaje": r["fecha_mensaje"],
            "has_thumb": int(r["has_thumb"] or 0),
            "thumb": thumb_info,
            "play_link": f"/play/{chat_id}/{message_id}",
            "thumb_url": (
                f"/api/photo/{file_id}?chat_id={chat_id}&video_id={video_id}" if file_id else None
            ),
        }
        groups[key]["items"].append(item)

    out_groups: list[dict[str, Any]] = []
    for _, g in groups.items():
        items = g.get("items") or []
        if len(items) >= min_group_size:
            out_groups.append(g)

    out_groups.sort(key=lambda x: len(x.get("items") or []), reverse=True)
    print(f"Grupos generados {len(out_groups)}")
    return {
        "criteria": {
            "by_name": by_name,
            "by_duration": by_duration,
            "by_video_size": by_video_size,
            "by_thumb_size": by_thumb_size,
            "duration_tol": duration_tol,
            "size_tol_bytes": size_tol_bytes,
            "thumb_mode": thumb_mode,
            "limit": limit,
            "min_group_size": min_group_size,
        },
        "scanned": len(rows or []),
        "sql": {
            "query": query_sql,
            "params": (limit, min_group_size, limit),
        },
        "groups": out_groups,
        "groups_count": len(out_groups),
    }


@router.post("/api/duplicates/hide")
async def hide_duplicates(
    payload: dict = Body(...),
):
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Se requiere al menos un item.")

    pairs: list[tuple[int, int]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            chat_id = int(it.get("chat_id"))
            message_id = int(it.get("message_id"))
        except Exception:
            continue
        pairs.append((chat_id, message_id))

    if not pairs:
        raise HTTPException(status_code=400, detail="Items inválidos.")

    try:
        async with get_db() as db:
            async with transaction(db) as cursor:
                await cursor.executemany(
                    """
                    UPDATE videos_telegram
                    SET oculto = 2
                    WHERE chat_id = ? AND message_id = ?
                    """,
                    pairs,
                )
        return {"ok": True, "updated": len(pairs)}
        
    except DatabaseConnectionError as e:
        logger.error(f"Error de conexión a la base de datos: {e}")
        raise HTTPException(
            status_code=503,
            detail="No se pudo conectar a la base de datos. Por favor, inténtelo de nuevo más tarde."
        )
    except Exception as e:
        logger.error(f"Error al ocultar duplicados: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error interno del servidor: {str(e)}"
        )
