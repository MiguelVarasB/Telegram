"""
Router de media agregado desde submódulos divididos.
"""
from fastapi import APIRouter

from .media_pages import router as media_pages_router
from .media_stream import router as media_stream_router
from .media_api import router as media_api_router

router = APIRouter()
router.include_router(media_pages_router)
router.include_router(media_stream_router)
router.include_router(media_api_router)

__all__ = ["router"]
