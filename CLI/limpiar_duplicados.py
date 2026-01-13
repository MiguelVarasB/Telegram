"""
Limpieza automática de duplicados en videos_telegram.

Criterios por defecto:
- Mismo nombre (normalizado)
- Misma duración
- Mismo tamaño de video (bytes)
- Misma thumb (ancho/alto o bytes)
- Semejanza de thumb >= 0.99 (si se habilita by_similarity)

Acciones:
- Marca los duplicados seleccionados con oculto = 3 (sin borrar).

Parámetros ajustables vía CLI.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import aiosqlite
from PIL import Image

try:
    import torch
    from torchvision import transforms
    from torchvision.models import ResNet18_Weights, resnet18
except Exception:
    torch = None  # type: ignore
    transforms = None  # type: ignore
    ResNet18_Weights = None  # type: ignore
    resnet18 = None  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from config import DB_PATH, THUMB_FOLDER  # noqa: E402
from routes.duplicates import _normalize_name
from utils.database_helpers import ensure_column  # noqa: E402

_SIM_MODEL = None
_SIM_TRANSFORM = None




def _thumb_info(chat_id: int, video_id: str, mode: str) -> Dict[str, Any]:
    thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
    if not os.path.exists(thumb_path):
        return {"exists": False, "bytes": None, "w": None, "h": None, "path": thumb_path}

    info: Dict[str, Any] = {"exists": True, "bytes": None, "w": None, "h": None, "path": thumb_path}

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
        raise RuntimeError("Torch/torchvision no están disponibles para semejanza.")

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


def _embed_thumb_similarity(path: str, device) -> Any:
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


def _cluster_by_similarity(items: List[Dict[str, Any]], similarity_threshold: float, device) -> Dict[int, int]:
    if torch is None:
        raise RuntimeError("Torch requerido para modo semejanza.")

    embed_list: List[Any] = []
    idx_list: List[int] = []

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

    root_to_id: Dict[int, int] = {}
    clusters: Dict[int, int] = {}
    next_id = 0
    for local_idx, original_idx in enumerate(idx_list):
        root = find(local_idx)
        if root not in root_to_id:
            root_to_id[root] = next_id
            next_id += 1
        clusters[original_idx] = root_to_id[root]

    return clusters


async def find_duplicates(
    by_name: bool,
    by_duration: bool,
    by_video_size: bool,
    by_thumb_size: bool,
    by_similarity: bool,
    duration_tol: int,
    size_tol_bytes: int,
    thumb_mode: str,
    similarity_threshold: float,
    limit: int,
    min_group_size: int,
) -> List[Dict[str, Any]]:
    thumb_mode = thumb_mode if thumb_mode in ("wh", "bytes") else "wh"
    device = None
    if by_similarity:
        device = "cuda" if (torch and torch.cuda.is_available()) else "cpu"

    # --- Filtros duros en SQL (etapa 1: nombre/duración/tamaño)
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
                file_unique_id, file_id, has_thumb, fecha_mensaje,
                {name_key_expr} AS key_name,
                {dur_key_expr} AS key_dur,
                {size_key_expr} AS key_size,
                {window_count} AS grp_count
            FROM videos_telegram
            {where_sql}
        )
        SELECT chat_id, message_id, nombre, tamano_bytes, duracion,
               file_unique_id, file_id, has_thumb, fecha_mensaje,
               key_name, key_dur, key_size
        FROM base
        WHERE grp_count >= ?
        ORDER BY fecha_mensaje DESC, message_id DESC
        LIMIT ?
    """

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query_sql, (min_group_size, limit)) as cursor:
            rows = await cursor.fetchall()

    rows = rows or []

    similarity_clusters: Dict[int, int] = {}
    if by_similarity:
        sim_items: List[Dict[str, Any]] = []
        for row_idx, r in enumerate(rows):
            chat_id = int(r["chat_id"])
            video_id = (r["file_unique_id"] or "").strip()
            thumb_path = os.path.join(THUMB_FOLDER, str(chat_id), f"{video_id}.webp")
            if not os.path.exists(thumb_path):
                continue
            sim_items.append({"row_idx": row_idx, "thumb_path": thumb_path})
        if sim_items:
            similarity_clusters = _cluster_by_similarity(sim_items, similarity_threshold, device)

    groups: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for row_idx, r in enumerate(rows):
        chat_id = int(r["chat_id"])
        message_id = int(r["message_id"])
        video_id = (r["file_unique_id"] or "").strip()
        nombre = r["nombre"]

        try:
            dur = int(round(float(r["duracion"] or 0)))
        except Exception:
            dur = 0

        try:
            size_bytes = int(r["tamano_bytes"] or 0)
        except Exception:
            size_bytes = 0

        # claves desde SQL; si no existen, se recalculan
        name_norm = _normalize_name(nombre) if by_name else None
        name_key: Any = r["key_name"] if by_name else None
        if by_name and not name_key:
            name_key = ("empty", chat_id, message_id)

        dur_key: Any = None
        if by_duration:
            if r["key_dur"] is not None:
                dur_key = int(r["key_dur"])
            elif duration_tol and duration_tol > 0:
                bucket = max(1, duration_tol)
                dur_key = int((dur // bucket) * bucket)
            else:
                dur_key = dur

        size_key: Any = None
        if by_video_size:
            if r["key_size"] is not None:
                size_key = int(r["key_size"])
            elif size_tol_bytes and size_tol_bytes > 0:
                bucket = max(1, size_tol_bytes)
                size_key = int((size_bytes // bucket) * bucket)
            else:
                size_key = size_bytes

        thumb_key: Any = None
        thumb_info: Dict[str, Any] | None = None
        if by_thumb_size:
            thumb_info = _thumb_info(chat_id, video_id, thumb_mode)
            if not thumb_info.get("exists"):
                thumb_key = ("missing", chat_id, message_id)
            elif thumb_mode == "bytes":
                thumb_key = thumb_info.get("bytes")
            else:
                thumb_key = (thumb_info.get("w"), thumb_info.get("h"))

        sim_key: Any = None
        if by_similarity:
            sim_key = similarity_clusters.get(row_idx)

        key_parts: List[Any] = []
        if by_name:
            key_parts.append(name_key)
        if by_duration:
            key_parts.append(dur_key)
        if by_video_size:
            key_parts.append(size_key)
        if by_thumb_size:
            key_parts.append(thumb_key)
        if by_similarity:
            key_parts.append(sim_key)

        key = tuple(key_parts)

        if key not in groups:
            groups[key] = {"items": []}

        groups[key]["items"].append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "video_id": video_id,
                "nombre": nombre or f"Video {message_id}",
                "duracion": dur,
                "tamano_bytes": size_bytes,
                "thumb": thumb_info,
            }
        )

    out_groups: List[Dict[str, Any]] = []
    for _, g in groups.items():
        items = g.get("items") or []
        if len(items) >= min_group_size:
            out_groups.append(g)

    return out_groups


async def marcar_oculto(groups: List[Dict[str, Any]]) -> int:
    """Marca oculto=3 en todos los items excepto el primero de cada grupo."""
    pairs: List[Tuple[int, int]] = []
    for g in groups:
        items = g.get("items") or []
        if len(items) < 2:
            continue
        # conservar el primero, marcar resto
        for it in items[1:]:
            pairs.append((int(it["chat_id"]), int(it["message_id"])))

    if not pairs:
        return 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE;")
        await db.executemany(
            """
            UPDATE videos_telegram
            SET oculto = 3
            WHERE chat_id = ? AND message_id = ?
            """,
            pairs,
        )
        await db.commit()
    return len(pairs)


async def main():
    parser = argparse.ArgumentParser(description="Limpieza automática de duplicados (marca has_thumb=3).")
    parser.add_argument("--by-name", action="store_true", default=True, help="Coincidencia por nombre normalizado.")
    parser.add_argument("--no-by-name", dest="by_name", action="store_false", help="Desactivar nombre.")
    parser.add_argument("--by-duration", action="store_true", default=True, help="Coincidencia por duración.")
    parser.add_argument("--no-by-duration", dest="by_duration", action="store_false", help="Desactivar duración.")
    parser.add_argument("--by-video-size", action="store_true", default=True, help="Coincidencia por tamaño de video.")
    parser.add_argument("--no-by-video-size", dest="by_video_size", action="store_false", help="Desactivar tamaño de video.")
    parser.add_argument("--by-thumb-size", action="store_true", default=True, help="Coincidencia por tamaño de thumb.")
    parser.add_argument("--no-by-thumb-size", dest="by_thumb_size", action="store_false", help="Desactivar tamaño de thumb.")
    parser.add_argument("--by-similarity", action="store_true", default=True, help="Coincidencia por semejanza de thumb.")
    parser.add_argument("--no-by-similarity", dest="by_similarity", action="store_false", help="Desactivar semejanza.")
    parser.add_argument("--duration-tol", type=int, default=0, help="Tolerancia de duración (s).")
    parser.add_argument("--size-tol-bytes", type=int, default=0, help="Tolerancia de tamaño (bytes).")
    parser.add_argument("--thumb-mode", choices=["wh", "bytes"], default="wh", help="Modo thumb (wh|bytes).")
    parser.add_argument("--similarity-threshold", type=float, default=0.99, help="Umbral de semejanza (0.5-0.99).")
    parser.add_argument("--limit", type=int, default=50000, help="Límite de filas a escanear.")
    parser.add_argument("--min-group-size", type=int, default=2, help="Tamaño mínimo de grupo duplicado.")
    args = parser.parse_args()

    try:
        groups = await find_duplicates(
            by_name=args.by_name,
            by_duration=args.by_duration,
            by_video_size=args.by_video_size,
            by_thumb_size=args.by_thumb_size,
            by_similarity=args.by_similarity,
            duration_tol=max(0, args.duration_tol),
            size_tol_bytes=max(0, args.size_tol_bytes),
            thumb_mode=args.thumb_mode,
            similarity_threshold=max(0.5, min(0.99, args.similarity_threshold)),
            limit=max(1, min(200_000, args.limit)),
            min_group_size=max(2, min(50, args.min_group_size)),
        )
    except Exception as e:
        print(f"❌ Error buscando duplicados: {e}")
        return

    total_groups = len(groups)
    print(f"Grupos encontrados: {total_groups}")
    total_items = sum(len(g.get("items", [])) for g in groups)
    print(f"Items en grupos: {total_items}")

    updated = await marcar_oculto(groups)
    print(f"✅ Marcados oculto=3: {updated}")


if __name__ == "__main__":
    asyncio.run(main())
