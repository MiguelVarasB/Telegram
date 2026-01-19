"""
MegaTelegram Local - Punto de entrada principal.
Aplicación modular para gestionar videos de Telegram.
Optimizado para arranque instantáneo (Non-blocking Warmup).
"""
import logging
import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from config import (
    ensure_directories, HOST, PORT, LOG_LEVEL, PYRO_LOG_LEVEL, UVICORN_LOG_LEVEL,
    MQTT_ENABLED, MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID, MQTT_USERNAME, MQTT_PASSWORD
)
from database import init_db
from services import start_client, stop_client, warmup_cache
from utils import init_mqtt_manager, get_mqtt_manager, log_timing
from routes import (
    home_router,
    folders_router,
    channels_router,
    media_router,
    media_api_router,
    sync_router,
    search_router,
    duplicates_router,
    tags_router,
)

# Logging: silenciar trazas ruidosas por defecto
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.WARNING))
for noisy_logger in (
    "pyrogram",
    "pyrogram.connection",
    "pyrogram.session",
    "pyrogram.session.auth",
):
    logging.getLogger(noisy_logger).setLevel(getattr(logging, PYRO_LOG_LEVEL, logging.ERROR))


async def background_warmup():
    """
    Tarea en segundo plano para calentar la caché de Telegram.
    Espera unos segundos para no competir con el arranque del servidor HTTP.
    """
    await asyncio.sleep(3)  # Espera 3s para que el servidor ya esté respondiendo
    log_timing("🔥 Ejecutando warmup de caché en segundo plano...")
    await warmup_cache(limit=100)
    log_timing("🔥 Warmup de caché finalizado.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestiona el ciclo de vida de la aplicación."""
    # --- STARTUP ---
    log_timing(" Iniciando MegaTelegram Local...")
    ensure_directories()
    
    # 1. Base de Datos
    # Se espera que 'init_db' ya esté optimizada (WAL mode) en database/connection.py
    await init_db() 
    
    # 2. MQTT (Inicialización segura)
    if MQTT_ENABLED:
        log_timing(f" Conectando al broker MQTT en {MQTT_BROKER}:{MQTT_PORT}...")
        try:
            mqtt_mgr = init_mqtt_manager(
                broker=MQTT_BROKER,
                port=MQTT_PORT,
                client_id=MQTT_CLIENT_ID,
                username=MQTT_USERNAME,
                password=MQTT_PASSWORD,
            )
            connected = await mqtt_mgr.connect()
            if connected:
                log_timing(" MQTT Manager inicializado correctamente")
            else:
                log_timing(" MQTT Manager no pudo conectarse (continuando sin MQTT)")
        except Exception as e:
            log_timing(f" Error iniciando MQTT: {e}")
    else:
        log_timing(" MQTT deshabilitado en configuración")
    
    # 3. Cliente Telegram
    # Usamos sesión de servidor para evitar conflictos con CLI
    await start_client(use_server_session=True)
    
    # 4. OPTIMIZACIÓN CRÍTICA: WARMUP NO BLOQUEANTE
    # En lugar de 'await warmup_cache', creamos una tarea independiente.
    # Esto permite que 'yield' se ejecute inmediatamente.
    asyncio.create_task(background_warmup())
    
    yield
    
    # --- SHUTDOWN ---
    await stop_client()
    
    if MQTT_ENABLED:
        mqtt_mgr = get_mqtt_manager()
        if mqtt_mgr:
            await mqtt_mgr.disconnect()


# Crear aplicación FastAPI
app = FastAPI(
    title="MegaTelegram Local",
    description="Gestión local de videos de Telegram",
    version="2.1.0",
    lifespan=lifespan,
)

# Registrar routers
app.include_router(home_router)
app.include_router(folders_router)
app.include_router(channels_router)
app.include_router(media_router)
app.include_router(media_api_router)
app.include_router(sync_router)
app.include_router(search_router)
app.include_router(duplicates_router)
app.include_router(tags_router)

# Montar archivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")


if __name__ == "__main__":
    log_timing(" Iniciando servidor Uvicorn...")
    uvicorn.run(app, host=HOST, port=PORT, log_level=UVICORN_LOG_LEVEL.lower())