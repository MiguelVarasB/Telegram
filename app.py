"""
MegaTelegram Local - Punto de entrada principal.
Aplicación modular para gestionar videos de Telegram.
"""
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from config import ensure_directories, HOST, PORT
from database import init_db
from services import start_client, stop_client, warmup_cache
from routes import (
    home_router,
    folders_router,
    channels_router,
    media_router,
    sync_router,
    search_router,
    duplicates_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestiona el ciclo de vida de la aplicación."""
    # Startup
    print(" Iniciando MegaTelegram Local...")
    ensure_directories()
    
    # --- INICIALIZACIÓN DE BASE DE DATOS ASÍNCRONA ---
    # Es crucial usar 'await' aquí porque cambiamos init_db a async
    await init_db() 
    
    # Usamos una sesión de Telegram separada para el servidor para evitar locks
    await start_client(use_server_session=True)
    
    await warmup_cache(limit=100)
    
    yield
    
    # Shutdown
    await stop_client()


# Crear aplicación FastAPI
app = FastAPI(
    title="MegaTelegram Local",
    description="Gestión local de videos de Telegram",
    version="2.0.0",
    lifespan=lifespan,
)

# Registrar routers
app.include_router(home_router)
app.include_router(folders_router)
app.include_router(channels_router)
app.include_router(media_router)
app.include_router(sync_router)
app.include_router(search_router)
app.include_router(duplicates_router)

# Montar archivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)