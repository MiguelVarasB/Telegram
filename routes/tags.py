from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, constr

from config import TEMPLATES_DIR, MAIN_TEMPLATE
from database import db_upsert_tag, db_list_tags
from utils import log_timing


class TagPayload(BaseModel):
    key: constr(strip_whitespace=True, min_length=1)
    name_en: constr(strip_whitespace=True, min_length=1)
    name_es: constr(strip_whitespace=True, min_length=1)


router = APIRouter(tags=["Tags"])
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/api/tags", summary="Lista todos los tags")
async def list_tags():
    log_timing(" Iniciando endpoint /api/tags..")
    tags = await db_list_tags()
    log_timing("Endpoint /api/tags terminado")
    return {"items": tags, "total": len(tags)}


@router.post("/api/tags", summary="Crea o actualiza un tag")
async def upsert_tag(payload: TagPayload):
    try:
        await db_upsert_tag(payload.key, payload.name_en, payload.name_es)
        return {"ok": True, "key": payload.key}
    except Exception as exc:  # pragma: no cover - captura genérica
        raise HTTPException(status_code=500, detail=f"Error guardando tag: {exc}")


@router.get("/tags", summary="Vista mantenedor de tags", include_in_schema=False)
async def tags_view(request: Request):
    log_timing(" Iniciando endpoint /tags..")
    tags = await db_list_tags()
    result = templates.TemplateResponse(
        MAIN_TEMPLATE,
        {
            "request": request,
            "view_type": "tags",
            "items": tags,
            "current_folder_name": "Tags",
            "current_folder_url": "/tags",
            "current_channel_name": None,
            "parent_link": "/",
        },
    )
    log_timing("Endpoint /tags terminado")
    return result
