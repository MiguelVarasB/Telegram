# Documentación del proyecto MegaTelegram Local

## Cambios recientes (dic 2025)
- Vista “Todos los canales” optimizada: consultas separadas (chats, conteos indexados, conteos totales, carpetas) y log_timing para medir; tiempo bajó a ~2.3s.
- Carga diferida de fotos de canal (placeholder + `data-src`).
- Modal de info de canal con botones “Indexar faltantes” y “Abrir canal”; API `/api/channel/{chat_id}/info` y `/api/channel/{chat_id}/scan`.
- WebSocket `/ws/folder/{folder_id}` envía `init` con items para cargar en segundo plano; frontend consume y re-renderiza.
- Instrumentación de rendimiento en frontend (performance marks) y logs de tiempo en backend (`log_timing`).
- **Refactor media:** `routes/media.py` se dividió en `media_common.py`, `media_pages.py`, `media_stream.py` y `media_api.py`, ensamblados por `media_router.py`.
- Nota: mantener este documento actualizado tras cada cambio relevante (backend, frontend, DB o scripts CLI).

## 1. Visión general

- **Backend:** FastAPI + Pyrogram con arquitectura modular.
- **Frontend:** Sistema de plantillas basado en `templates/layout.html` + parciales en `templates/partials/` para:
  - Vista de carpetas (chats).
  - Vista de archivos (videos).
  - Reproductor de video (modal de video).
- **BD local:** SQLite en `database/chats.db` (configurable vía `DB_PATH`), gestionada con `aiosqlite`.
- **Flujo Dump/Thumbs:** el proyecto soporta un flujo en 2 pasos:
  - `CLI/reenviar_videos.py` reenvía mensajes de videos hacia `CACHE_DUMP_VIDEOS_CHANNEL_ID` y guarda `dump_message_id`.
  - `services/thumb_worker_hibrido.py` descarga thumbnails desde el canal de dump usando bots (`BOT_POOL_TOKENS`).
  - `CLI/descargar_dump.py` ahora guarda también el tamaño en bytes del thumb (`thumb_bytes`) al momento de descargar/convertir el WebP.
- **Archivos auxiliares:**
  - Carpeta `dumps/` con:
    - `json/folder_dump_{id}.json` (estado serializado de carpetas).
    - `json/raw_dump_{chat_id}.json` (estado serializado de canales con videos).
    - `json/reporte_stats_*.json` (reportes generados por scripts de descarga).
    - `thumbs/videos/{chat_id}/{file_unique_id}.webp` para miniaturas de videos por canal.
    - `thumbs/grupos_canales/{file_id}.webp` para fotos de grupos/canales.
    - `videos/{chat_id}/{video_id}.mp4` para descargas completas en disco SSD.
    - `smart_cache/{video_id}.cache` para el caché inteligente en disco (previsualización parcial).
  - `migrations/001_create_videos_tables.py` para las tablas de videos (estructura base).

---

## 2. Arquitectura Modular

### 2.1 Estructura del Proyecto

```
Telegram/
├── app.py                     # Punto de entrada principal
├── config.py                  # Configuración centralizada
├── run_cli.py / run_cli.bat   # Lanzador interactivo para scripts en CLI/
├── bot_videos.py              # Bot para manejo de videos
├── migrar_db.py               # Script de migración de base de datos
├── test.py                    # Script de pruebas
├── sitios_soportados.txt      # Lista de sitios soportados
├── iniciar_descarga_thumbs.bat # Batch para iniciar descarga de thumbnails
├── CLI/                       # Scripts para ejecutar desde terminal (~17 items)
│   ├── descargar_dump.py
│   ├── reenviar_videos.py
│   ├── scan_all_channels_to_db.py
│   ├── sincronizar_db.py
│   └── ... (otros scripts)
├── database/                  # Módulo de base de datos + archivo SQLite (7 items)
│   ├── __init__.py            # Exporta funciones públicas
│   ├── connection.py          # Conexión SQLite (async) e init_db()
│   ├── chats.py               # Operaciones de chats (async)
│   ├── videos.py              # Operaciones de videos y mensajes (async)
│   ├── folders.py             # Operaciones de carpetas (async)
│   ├── connection.py.bak      # Respaldo
│   └── chats.db               # Base de datos local (DB_PATH)
├── services/                  # Módulo de servicios (8 items)
│   ├── __init__.py            # Exporta servicios públicos
│   ├── telegram_client.py     # Cliente Pyrogram singleton
│   ├── folder_sync.py         # Sincronización de carpetas manuales
│   ├── video_streamer.py      # Streaming de video híbrido (Disco/RAM/Telegram)
│   ├── memory_cache.py        # Caché en RAM de videos (buffers + metadatos)
│   ├── disk_cache.py          # Caché inteligente en disco (LRU)
│   ├── prefetch.py            # Pre-carga de videos a Disco/RAM
│   └── thumb_worker_hibrido.py # Worker híbrido de thumbnails
├── routes/                    # Módulo de rutas FastAPI (13 items)
│   ├── __init__.py            # Exporta routers
│   ├── home.py                # Ruta /
│   ├── folders.py             # Rutas /folder/, /api/folder/, websocket
│   ├── channels.py            # Ruta /channel/ (escaneo + guardado en BD)
│   ├── media.py               # Reexporta router compuesto de media
│   ├── media_router.py        # Incluye subrouters de media
│   ├── media_common.py        # Helpers compartidos (cachés, semáforos)
│   ├── media_pages.py         # Páginas /videos, /watch_later, /play
│   ├── media_stream.py        # Streaming y descargas de video
│   ├── media_api.py           # API de thumbnails, stats, watch_later
│   ├── sync.py                # Ruta /sync/diario (sincronización incremental)
│   ├── search.py              # Rutas de búsqueda
│   └── duplicates.py          # Rutas de duplicados
├── utils/                     # Módulo de utilidades (5 items)
│   ├── __init__.py            # Exporta helpers
│   ├── helpers.py             # Funciones auxiliares
│   ├── websocket.py           # FolderWSManager
│   ├── media_processor.py     # Procesador multimedia (FFmpeg)
│   └── transcoder.py          # Transcodificador adicional
├── templates/                 # Plantillas frontend (14 items)
│   ├── layout.html            # Template principal
│   ├── mega_ui.html           # Template monolítico anterior
│   └── partials/              # Fragmentos reutilizables
│       ├── top_bar.html
│       ├── content.html
│       ├── video_modal.html
│       ├── styles.html
│       ├── scripts.html
│       ├── grid.html
│       ├── card_video.html
│       ├── card_folder.html
│       ├── player.html
│       └── ... (otros 5 items)
├── dumps/                     # Datos locales
│   ├── json/
│   ├── thumbs/
│   ├── videos/
│   └── smart_cache/
├── migrations/                # Migraciones de base de datos (1 item)
│   └── 001_create_videos_tables.py
├── static/                    # Archivos estáticos
├── sessions/                  # Sesiones de Telegram
├── downloads/                 # Descargas temporales
└── .test/                     # Directorio de pruebas
```

### 2.2 Módulo `config.py`

Centraliza toda la configuración:

```python
import os

# Telegram API
API_ID = 24228679
API_HASH = "..."
SESSION_NAME = "mi_sesion_premium"

# BOTS (Pool)
BOT_TOKEN = "..."
BOT_POOL_TOKENS = [
    BOT_TOKEN,
]

# Canal de dump para videos (cache)
CACHE_DUMP_VIDEOS_CHANNEL_ID = -1000000000000

# STREAMING
CHUNK_SIZE = 1024 * 1024  # 1MB

# SMART CACHE (Disco)
MAX_DISK_CACHE_SIZE = 2 * 1024 * 1024 * 1024  # Límite total de la carpeta smart_cache (2GB)
TARGET_VIDEO_CACHE_SIZE = 5 * 1024 * 1024     # Tamaño objetivo por video (5MB aprox.)

# RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUMP_FOLDER = os.path.join(BASE_DIR, "dumps")
JSON_FOLDER = os.path.join(DUMP_FOLDER, "json")
FOLDER_SESSIONS = os.path.join(BASE_DIR, "sessions")
THUMB_FOLDER = os.path.join(DUMP_FOLDER, "thumbs", "videos")
GRUPOS_THUMB_FOLDER = os.path.join(DUMP_FOLDER, "thumbs", "grupos_canales")
CACHE_DIR = os.path.join(DUMP_FOLDER, "smart_cache")

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")  # Carpeta donde viven layout.html y los parciales
MAIN_TEMPLATE = "layout.html"                         # Template principal de la interfaz web

# Base de datos
DB_PATH = os.path.join(BASE_DIR, "database", "chats.db")

# Servidor
HOST = "127.0.0.2"
PORT = 8000

# FFMPEG / SPRITES (Procesamiento multimedia)
# Dimensiones solicitadas
SPRITE_COLS = 5
SPRITE_ROWS = 15
SPRITE_THUMB_WIDTH = 400

# Configuraciones por defecto
THUMB_WIDTH = 320
THUMB_QUALITY = 80
SPRITE_QUALITY = 70  # Calidad un poco menor para que el sprite no pese demasiado
MIN_SPRITE_DURATION = 5
MIN_FILE_SIZE = 1024

# Timeouts (segundos)
FFPROBE_TIMEOUT = 15
FFMPEG_THUMB_TIMEOUT = 30
FFMPEG_SPRITE_TIMEOUT = 180  # Aumentado para sprites grandes (400px * 5 * 15 es grande)

def ensure_directories():
    """Crea las carpetas necesarias (dumps, thumbs, smart_cache)."""
    for folder in [DUMP_FOLDER, JSON_FOLDER, THUMB_FOLDER, GRUPOS_THUMB_FOLDER, CACHE_DIR, FOLDER_SESSIONS]:
        os.makedirs(folder, exist_ok=True)
```

### 2.2.1 Scripts de terminal (carpeta `CLI/`)

Los scripts pensados para ejecutarse desde terminal viven en `CLI/`.

Formas recomendadas de ejecutarlos:

1) Lanzador interactivo (recomendado)

```bash
py run_cli.py
```

2) Ejecutar un script específico

```bash
py CLI/reenviar_videos.py
py CLI/descargar_dump.py
py CLI/sincronizar_db.py
py CLI/scan_all_channels_to_db.py
```

Notas:

- Los scripts de `CLI/` agregan automáticamente la raíz del proyecto al `sys.path`, para que funcionen aunque los ejecutes como `py CLI/archivo.py`. Además, el lanzador `run_cli.py` ahora exporta `PYTHONPATH` con la raíz del proyecto al ejecutar cualquier script, evitando errores de import (`config`, etc.) aunque arranques desde otra carpeta.
- Los JSON generados por la app y por scripts de `CLI/` se guardan en `dumps/json/` (configurable con `JSON_FOLDER`).
- Si usas `iniciar_descarga_thumbs.bat`, debe invocar los scripts dentro de `CLI/` (por ejemplo `py CLI\reenviar_videos.py` y `py CLI\descargar_dump.py`).

### 2.2.2 Buscador (frontend)

- El buscador global/local (barra superior) obtiene hasta 20 resultados por petición (`/api/search`).
- Para evitar confusión con la paginación del listado general de videos, al iniciar una búsqueda se elimina la barra de paginación previa y solo se muestran los resultados retornados. El título de sección se actualiza a `Resultados (N)`.

### 2.3 Módulo `database/`

#### `connection.py`
- `get_connection()` → Retorna conexión SQLite **síncrona** (uso legacy o scripts puntuales).
- `init_db()` → **Async**. Crea tablas base si no existen y garantiza que la tabla `chats` tenga la columna `last_message_date`.

#### `chats.py`
- `db_upsert_chat_from_ci(ci, last_message_date)` → Guarda/actualiza un chat resuelto (`Chat` de Pyrogram) en la tabla `chats` (async).
- `db_get_chat(chat_id)` → Obtiene un chat por ID (async).
- `db_get_chat_folders(chat_id)` → Lista las carpetas a las que pertenece un chat (async).
- `db_add_chat_folder(chat_id, folder_id)` → Añade relación chat-carpeta (async).

#### `videos.py`
- `db_upsert_video(video_data)` → Inserta/actualiza video en `videos_telegram` (async).
- `db_add_video_file_id(video_id, file_id, file_unique_id, origen)` → Registra `file_id` históricos en `video_file_ids` (async).
- `db_upsert_video_message(message_data)` → Inserta/actualiza metadatos extendidos de mensajes en `video_messages` (async).
- `db_get_video_messages(video_id)` → Devuelve la lista de mensajes asociados a un video (async).
- `db_count_videos_by_chat(chat_id)` → Devuelve el número de videos en la base de datos para un chat específico.

#### `folders.py`
- `get_folder_items_from_db(folder_id)` → Lista chats de una carpeta (async).

### 2.4 Módulo `services/`

#### `telegram_client.py`
- `get_client()` → Retorna el cliente Pyrogram singleton.
- `start_client()` / `stop_client()` → Inicia / detiene el cliente.
- `warmup_cache(limit)` → Calienta caché de diálogos.
- `reconnect_client()` → Reconecta el cliente de Telegram de forma segura cuando se corta la conexión.
- `ensure_connected()` → Verifica que el cliente esté conectado y, si no, intenta reconectar automáticamente.
- `with_reconnect(coro_func, *args, max_retries=3, **kwargs)` → Ejecuta una coroutine de Pyrogram con reintentos y reconexión automática ante errores de socket (WinError 10053, etc.).

#### `thumb_worker_hibrido.py`
- Descarga thumbnails de videos (`has_thumb = 0`) usando el canal de dump (`CACHE_DUMP_VIDEOS_CHANNEL_ID`).
- Requiere que `dump_message_id` esté poblado (mensaje del video dentro del canal de dump).
- Usa bots del pool (`BOT_POOL_TOKENS`) para descargar el thumb con alta concurrencia.
- `background_thumb_downloader()`: Función que se ejecuta en segundo plano para descargar thumbnails.

#### `folder_sync.py`
- `refresh_manual_folder_from_telegram(folder_id, name)` → Sincroniza una carpeta **manual** con Telegram, actualizando la BD y notificando por WebSocket.

#### `video_streamer.py`
- `TelegramVideoSender` → Clase para streaming híbrido con soporte de rangos HTTP.
  - Soporta 3 niveles: **Disco completo**, **Smart Cache (Disco/RAM)** y **Telegram** como fallback.
  - `setup(max_retries=3)` → Obtiene el mensaje y metadatos de video con reintentos y reconexión automática usando `ensure_connected()` y `reconnect_client()`.

#### `memory_cache.py`
- `store_in_ram(video_id, data, total_size, mime_type, message_obj=None)` → Guarda en RAM el buffer parcial/completo de un video + metadatos + objeto mensaje.
- `get_from_ram(video_id)` → Recupera el objeto de caché (data + metadatos).
- `clear_ram_cache()` / `get_ram_usage_count()` → Mantenimiento de la caché en memoria.

#### `disk_cache.py`
- `save_to_disk_smart(video_id, data)` → Guarda datos en disco dentro de `smart_cache/`, respetando `MAX_DISK_CACHE_SIZE` con política LRU.
- `get_cache_path(video_id)` → Ruta física del archivo de caché.
- `touch_file(video_id)` → Actualiza la fecha de último acceso para evitar que se borre pronto.

#### `prefetch.py`
- `prefetch_channel_videos_to_ram(chat_id)` → Lee los videos de un canal desde la BD (`videos_telegram`) y asegura que haya un buffer de ~5MB por video en Disco/RAM usando `SmartCache`.

### 2.5 Módulo `utils/`

#### `helpers.py`
- `obtener_id_limpio(peer)` → Extrae ID limpio de peer.
- `convertir_tamano(size_bytes)` → Convierte bytes a MB/GB.
- `formatear_miles(value)` → Formatea números con `.` como separador de miles (no usar para IDs).
- `json_serial(obj)` → Serializa datetime para JSON.
- `serialize_pyrogram(obj)` → Serializa objetos Pyrogram.
- `save_image_as_webp(source, dest)` → Convierte imagen a WebP.
- `force_resolve_peer(client, peer)` → Resuelve peer con Raw API.

#### `websocket.py`
- `FolderWSManager` → Gestiona conexiones WebSocket por carpeta.
- `ws_manager` → Instancia global del manager.

#### `media_processor.py`
- `MediaProcessor` → Wrapper sobre FFmpeg/FFprobe (con detección opcional de GPU CUDA) para procesamiento de videos:
  - `obtener_metadatos_completos(url_stream)` → Usa `ffprobe` para extraer duración, ancho, alto, codec de video y si es vertical.
  - `generar_sprite(url_stream, ruta_destino, duracion)` → Genera un sprite WebP en grilla (`SPRITE_COLS` x `SPRITE_ROWS`) usando los parámetros de `config.py`.
  - `async_obtener_metadatos(...)` / `async_generar_sprite(...)` → Wrappers asíncronos (`asyncio.to_thread`) pensados para ser usados desde FastAPI sin bloquear el event loop.

### 2.6 Módulo `routes/`

#### `home.py` - Ruta `/`
- Lista carpetas del usuario: Inbox (0), Archivados (1) y carpetas personalizadas (filtros de Telegram).
- Incluye un acceso directo **fijo** a `Videos` (primero en la lista) que lleva a `GET /videos`.
- El conteo mostrado en `Videos` corresponde a la **cantidad total de videos** con thumb (`has_thumb = 1`).

#### `folders.py` - Rutas de carpetas
- `GET /folder/{folder_id}` → Vista de carpeta (lista de chats) apoyada en la BD; para carpetas manuales dispara un refresco en background.
- `GET /api/folder/{folder_id}` → API JSON de carpeta (usado por el frontend para refrescos en vivo).
- `WS /ws/folder/{folder_id}` → WebSocket para enviar eventos `{"type": "refresh"}` al frontend cuando cambian los datos.

#### `channels.py` - Ruta `/channel/{chat_id}`
- Escanea videos del canal (solo mensajes con `video`), los guarda en `videos_telegram` y `video_messages` de forma asíncrona y genera un `raw_dump_{chat_id}.json` en `dumps/json/`.
- Lanza `prefetch_channel_videos_to_ram(chat_id)` en `BackgroundTasks` para precargar buffers en Disco/RAM.

#### `media.py` - Rutas de media y streaming
- `GET /videos` → Vista global de videos indexados con **paginación**. Solo muestra videos con thumbnail descargado.
  - Query params:
    - `page` (default 1)
    - `per_page` (default 60)
  - Criterio de thumb: `has_thumb = 1` y existe archivo en `dumps/thumbs/videos/{chat_id}/{file_unique_id}.webp`.
- `GET /play/{chat_id}/{message_id}` → Página del reproductor, detectando si el video ya está descargado en disco.
- `GET /video_stream/{chat_id}/{message_id}` → Streaming híbrido Disco → RAM → Telegram con soporte de rangos HTTP.
- `POST /api/download/{chat_id}/{message_id}` → Dispara la descarga completa del video al SSD en segundo plano.
- `POST /api/prefetch/{chat_id}` → Fuerza manualmente la pre-carga de videos de un canal a Disco/RAM.
- `GET /api/video/{video_id}/messages` → Devuelve los mensajes asociados a un video desde la tabla `video_messages`.
- `GET /api/photo/{file_id}` → Caché de imágenes con conversión a WebP.

#### `sync.py` - Rutas de sincronización
- `POST /sync/diario` → Ejecuta la sincronización incremental de chats para Inbox (folder_id 0) y Archivados (folder_id 1) reutilizando `GetDialogs` y guardando en `chats` + `chat_folders` solo los diálogos recientes (últimas 24h).

#### `search.py` - Rutas de búsqueda
- `GET /search` → Busca videos por término de búsqueda (título, etiquetas, etc.).

#### `duplicates.py` - Rutas de duplicados
- `GET /duplicates` → Lista videos duplicados (basado en hash o contenido similar).
- `POST /duplicates/merge` → Fusiona videos duplicados.

---

## 3. Punto de Entrada (`main.py`)

```python
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

from config import ensure_directories, HOST, PORT
from database import init_db
from services import start_client, stop_client, warmup_cache
from routes import home_router, folders_router, channels_router, media_router, sync_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestiona el ciclo de vida de la aplicación."""
    # Startup
    print(" Iniciando MegaTelegram Local...")
    ensure_directories()

    # Inicialización de base de datos ASÍNCRONA
    await init_db()

    await start_client()
    await warmup_cache(limit=100)

    yield

    # Shutdown
    await stop_client()


app = FastAPI(
    title="MegaTelegram Local",
    description="Gestión local de videos de Telegram",
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(home_router)
app.include_router(folders_router)
app.include_router(channels_router)
app.include_router(media_router)
app.include_router(sync_router)
```

---

## 4. Base de Datos SQLite

La base de datos vive en `database/chats.db` (ruta configurada en `config.DB_PATH`) y se inicializa de forma **asíncrona** mediante `init_db()` usando `aiosqlite`. Durante el arranque se aseguran las tablas base y, si hace falta, se añade la columna `last_message_date` a la tabla `chats`.

Nota: `DB_PATH` se define como ruta absoluta (usando `BASE_DIR`) para que los scripts de `CLI/` puedan abrir la BD sin depender del directorio de ejecución (working directory).

### 4.1 Tablas principales

```sql
-- Chats
CREATE TABLE chats (
    chat_id INTEGER PRIMARY KEY,
    name TEXT,
    type TEXT,
    photo_id TEXT,
    username TEXT,
    raw_json TEXT,
    last_message_date TEXT,
    updated_at TEXT
);

-- Relación chat-carpeta
CREATE TABLE chat_folders (
    chat_id INTEGER,
    folder_id INTEGER,
    PRIMARY KEY (chat_id, folder_id)
);
```

### 4.2 Tablas de videos (migración 001)

```sql
-- Videos únicos
CREATE TABLE videos_telegram (
    id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL,
    nombre TEXT,
    caption TEXT,
    tags_ia TEXT DEFAULT '[]',
    meta_extra TEXT,
    ruta_local TEXT,
    ruta_mega TEXT,
    tamano_bytes INTEGER,
    fecha_mod TEXT,
    fecha_mensaje TEXT,
    fecha_descarga TEXT,
    fecha_procesado TEXT DEFAULT '',
    duracion REAL DEFAULT 0,
    ancho INTEGER DEFAULT 0,
    alto INTEGER DEFAULT 0,
    es_vertical INTEGER DEFAULT 0,
    codec_video TEXT DEFAULT '',
    codec_audio TEXT DEFAULT '',
    bitrate INTEGER DEFAULT 0,
    fps REAL DEFAULT 0,
    tiene_audio INTEGER DEFAULT 1,
    version_sprite TEXT DEFAULT '',
    es_video INTEGER DEFAULT 0,
    has_sprite INTEGER DEFAULT 0,
    has_thumb INTEGER DEFAULT 0,
    dump_message_id INTEGER,
    dump_fail INTEGER DEFAULT 0,
    en_mega INTEGER DEFAULT 1,
    oculto INTEGER DEFAULT 0,
    ffmpeg_error INTEGER DEFAULT 0,
    mime_type TEXT,
    views INTEGER DEFAULT 0,
    outgoing INTEGER DEFAULT 0,
    reply_to_message_id INTEGER,
    forwarded_from TEXT,
    sprite_path TEXT,
    thumb_path TEXT,
    download_url TEXT,
    UNIQUE (chat_id, message_id),
    UNIQUE (file_unique_id)
);

-- Historial de file_ids
CREATE TABLE video_file_ids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT,
    file_id TEXT,
    file_unique_id TEXT,
    fecha_detectado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    origen TEXT,
    notas TEXT,
    UNIQUE (video_id, file_id)
);
```
### 4.3 Tabla de mensajes de video (`video_messages`)

Esta tabla almacena los metadatos extendidos de los mensajes que contienen cada video (quién lo envió, forwards, vistas, etc.), alimentada por `db_upsert_video_message()`:

```sql
CREATE TABLE video_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    date TEXT,
    from_user_id INTEGER,
    from_username TEXT,
    from_is_bot INTEGER,
    media_type TEXT,
    views INTEGER,
    forwards INTEGER,
    outgoing INTEGER,
    reply_to_message_id INTEGER,
    forward_from_chat_id INTEGER,
    forward_from_chat_title TEXT,
    forward_from_message_id INTEGER,
    forward_date TEXT,
    caption TEXT,
    UNIQUE (chat_id, message_id)
);
```

---

## 5. Frontend: templates/layout.html + parciales

El frontend ahora se organiza en:

- `templates/layout.html`: plantilla base con bloques (`layout`, `top_bar`, `main_content`, `video_modal`, `scripts`).
- `templates/partials/`: fragmentos reutilizables del layout:
  - `top_bar.html` (barra superior, logo, breadcrumb, filtros y acciones).
  - `content.html` (contenedor principal que usa `grid.html` o `player.html` según `view_type`).
  - `grid.html`, `card_video.html`, `card_folder.html` (tarjetas de videos y carpetas/chats).
  - `video_modal.html` (modal de video + mensajes asociados).
  - `styles.html` (CSS principal del layout).
  - `scripts.html` (JS principal: grid, modal, filtros, ordenamiento, ocultar duplicados).

### 5.1 Layout general

- Tema oscuro tipo MEGA, con un contenedor principal:
  - `.browser-container`: tarjeta grande centrada, fondo #181818, bordes redondeados.
  - `.top-bar`: barra fija arriba del contenedor con logo, breadcrumb y acciones.

- Barra superior:
  - Botón redondo de volver (`.btn-back`) si existe `parent_link`.
  - Logo `MegaTelegram` con icono de nube.
  - `breadcrumb`: muestra `Ruta: {{ current_folder_name }}` truncado.
  - Si `view_type == 'folder'` muestra un bloque de acciones con un checkbox:
    - "Ocultar canales/grupos con error" (`#toggle-errors`).

### 5.2 Grid y tarjetas

- `.grid-view`: grilla responsiva con tarjetas `.file-item`.
- Cada tarjeta tiene:
  - `.icon-box` (diferente para videos vs carpetas).
  - `.file-name`: nombre (truncado, `title` con valor completo).
  - `.file-info`: texto auxiliar (`item.count`).

#### Videos

- Si `item.type == 'video'`:
  - `.icon-box.video-box`:
    - Si `item.photo_id` → `<img class="video-thumb">` ocupando todo el 16:9.
    - Si no → icono grande `fa-file-video`.

#### Canales/carpetas

- Si el item NO es video (por ejemplo chats, carpetas raíz/manuales):
  - `.icon-box.folder-box` con fondo degradado.
  - Icono de carpeta grande:
    - `folder-root` para vista raíz.
    - `folder-chat` para vista de carpetas.
  - Si tiene `photo_id` → se renderiza una imagen circular en la esquina inferior derecha (`.folder-overlay`) con la foto del grupo/canal.

De esta forma los canales/carpetas se distinguen visualmente de los videos.

### 5.3 Formato de números (UI)

Para mantener consistencia visual en la interfaz:

- Se formatean los números con `.` como separador de miles (por ejemplo `12.345`).
- **No se formatean IDs** (por ejemplo `chat_id`, `message_id`, `video_id`) para evitar confusiones.

Implementación:

- Backend (Jinja/FastAPI): helper `formatear_miles()` y/o valores `*_fmt` en el contexto del template.
- Frontend (JS): función `formatThousands()`.

### 5.4 Lógica JavaScript en `templates/partials/scripts.html`

#### Filtro de errores

- Checkbox `#toggle-errors`, solo visible en vista de carpeta.
- Función `applyErrorFilter()`:
  - Recorre todos los `.file-item`.
  - Si `file-info` contiene `"❌ Error"`, oculta o muestra según estado del checkbox.

#### WebSocket y refresco en vivo para carpetas manuales

- Lee `data-folder-id` del contenedor principal cuando `current_folder_id` está definido.
- Si hay `folderId`:
  - Abre un WebSocket a `/ws/folder/{folderId}`.
  - Cuando recibe un mensaje `{"type": "refresh"}`:
    - Llama `fetch('/api/folder/' + folderId)` y recibe un JSON con items.
    - Vuelve a construir la grilla usando la misma lógica de iconos que en la plantilla (videos vs carpetas con foto).
    - Reaplica `applyErrorFilter()` para mantener filtrados los items con error.

## Resumen de Diferencias

Este documento fue actualizado el 23 de diciembre de 2025 para reflejar el estado actual del código. Las principales diferencias identificadas entre la documentación anterior y el código son:

1. **Archivos nuevos en la raíz del proyecto:**
   - `bot_videos.py`: Bot para manejo de videos.
   - `iniciar_descarga_thumbs.bat`: Batch para iniciar descarga de thumbnails.
   - `migrar_db.py`: Script de migración de base de datos.
   - `run_cli.bat`: Script batch para ejecutar run_cli.py.
   - `sitios_soportados.txt`: Lista de sitios soportados.
   - `test.py`: Script de pruebas.

2. **Directorios nuevos:**
   - `downloads/`: Descargas temporales.
   - `sessions/`: Sesiones de Telegram.
   - `static/`: Archivos estáticos.
   - `.test/`: Directorio de pruebas.

3. **Actualizaciones en directorios existentes:**
   - `CLI/`: Ahora contiene 12 scripts (antes 4).
   - `database/`: Ahora contiene 5 archivos (antes 6).
   - `services/`: Ahora contiene 8 archivos (antes 7).
   - `routes/`: Ahora contiene 8 archivos (antes 6).
   - `templates/`: Ahora contiene 14 archivos (antes 13).
   - `utils/`: Ahora contiene 5 archivos (antes 4).

Estos cambios aseguran que la documentación refleje con precisión la estructura actual del proyecto.
