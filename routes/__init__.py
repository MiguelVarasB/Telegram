"""
Módulo de rutas FastAPI.
"""
from .home import router as home_router
from .folders import router as folders_router
from .channels import router as channels_router
from .media import router as media_router
from .media_api import router as media_api_router
from .sync import router as sync_router
from .search import router as search_router
from .duplicates import router as duplicates_router
from .tags import router as tags_router

__all__ = [
    "home_router",
    "folders_router",
    "channels_router",
    "media_router",
    "media_api_router",
    "sync_router",
    "search_router",
    "duplicates_router",
    "tags_router",
]
