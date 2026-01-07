import asyncio
import aiosqlite

from config import DB_PATH

__all__ = [
    "thumb_download_sem",
    "video_info_cache",
    "thumb_db_cache",
    "MAX_CACHE_SIZE",
    "get_video_info_from_db",
    "_format_duration",
    "_build_page_links",
]

# --- CONTROL DE CONCURRENCIA ---
# Limitamos a 3 descargas simultáneas de thumbnails para evitar auth.ExportAuthorization
thumb_download_sem = asyncio.Semaphore(3)

# --- CACHÉ EN MEMORIA ---
# Caché para evitar consultas repetidas a la BD
video_info_cache: dict[str, tuple | None] = {}
MAX_CACHE_SIZE = 1000

thumb_db_cache: dict[str, tuple | None] = {}


# --- UTILS DB ASYNC ---
async def get_video_info_from_db(chat_id: int, message_id: int, file_unique_id: str | None = None):
    """
    Busca de forma ASÍNCRONA si el video ya está descargado por completo en disco.
    Usa caché en memoria para evitar consultas repetidas.
    """
    cache_key = f"{chat_id}:{message_id}:{file_unique_id or ''}"

    # 1. Intentar obtener de la caché
    if cache_key in video_info_cache:
        return video_info_cache[cache_key]

    # 2. Si no está en caché, consultar BD
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            if file_unique_id:
                query = "SELECT id, ruta_local FROM videos_telegram WHERE chat_id = ? AND message_id = ? AND file_unique_id = ?"
                params = (chat_id, message_id, file_unique_id)
            else:
                query = "SELECT id, ruta_local FROM videos_telegram WHERE chat_id = ? AND message_id = ?"
                params = (chat_id, message_id)

            async with db.execute(query, params) as cursor:
                row = await cursor.fetchone()
                if row:
                    # Guardar en caché
                    result = (row[0], row[1])  # video_id, ruta_local
                    if len(video_info_cache) >= MAX_CACHE_SIZE:
                        # Limpiar caché si es muy grande
                        video_info_cache.clear()
                    video_info_cache[cache_key] = result
                    return result
    except Exception as e:
        print(f"Error DB info: {e}")

    # Guardar el negativo también en caché
    video_info_cache[cache_key] = (None, None)
    return None, None


def _format_duration(seconds):
    if not seconds:
        return ""
    try:
        mins, secs = divmod(int(seconds), 60)
        hours, mins = divmod(mins, 60)
        if hours:
            return f"{hours}:{mins:02d}:{secs:02d}"
        return f"{mins}:{secs:02d}"
    except Exception:
        return ""


def _build_page_links(page: int, total_pages: int):
    """
    Genera una lista de páginas con el patrón clásico 1 .. N.
    Reglas:
    - Si hay pocas páginas (<=12), las muestra todas.
    - Si el usuario está al inicio (página <= 8), muestra 1..10, elipsis y las últimas 2.
    - Si está al final (página >= total_pages-7), muestra 1,2, elipsis y las últimas 10.
    - En el medio, muestra 1,2, elipsis, una ventana centrada en la página (5 ítems), elipsis y las últimas 2.
    """
    if total_pages <= 12:
        return list(range(1, total_pages + 1))

    pages = []

    def _push(n):
        if n is None:
            pages.append(None)
            return
        if 1 <= n <= total_pages and n not in pages:
            pages.append(n)

    if page <= 8:
        for n in range(1, 11):
            _push(n)
        _push(None)  # elipsis
        _push(total_pages - 1)
        _push(total_pages)
    elif page >= total_pages - 7:
        _push(1)
        _push(2)
        _push(None)
        for n in range(total_pages - 9, total_pages + 1):
            _push(n)
    else:
        _push(1)
        _push(2)
        _push(None)
        for n in range(page - 2, page + 3):
            _push(n)
        _push(None)
        _push(total_pages - 1)
        _push(total_pages)

    # Limpiar posibles duplicados y ordenar, preservando elipsis
    cleaned = []
    prev_num = None
    for n in pages:
        if n is None:
            if cleaned and cleaned[-1] is not None:
                cleaned.append(None)
            prev_num = None
        else:
            if n != prev_num:
                cleaned.append(n)
                prev_num = n
    if cleaned and cleaned[-1] is None:
        cleaned.pop()
    return cleaned
