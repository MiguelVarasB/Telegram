"""
Consultas de lectura (Read Operations) para la interfaz web.
Separa la lógica SQL de las rutas.
"""
import aiosqlite
from config import DB_PATH

async def db_get_chat_info(chat_id: int):
    """Obtiene información básica de un chat."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, title, username, type FROM chats WHERE id = ?", 
            (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def db_get_channel_videos(
    chat_id: int, 
    limit: int = 50, 
    offset: int = 0, 
    search_query: str = "", 
    sort_order: str = "date_desc", 
    filter_type: str = "all"
):
    """
    Obtiene videos de un canal con filtros, búsqueda y paginación.
    Retorna: (lista_videos, total_count)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # Construcción dinámica de la query
        where_clauses = ["chat_id = ?"]
        params = [chat_id]

        # 1. Filtros
        if filter_type == "no_thumb":
            where_clauses.append("has_thumb = 0")
        elif filter_type == "vertical":
            where_clauses.append("es_vertical = 1")
        elif filter_type == "long":
            where_clauses.append("duracion >= 3600")

        # 2. Búsqueda
        if search_query:
            where_clauses.append("(file_name LIKE ? OR caption LIKE ?)")
            q_str = f"%{search_query}%"
            params.extend([q_str, q_str])

        where_sql = " AND ".join(where_clauses)

        # 3. Ordenamiento
        sort_map = {
            "date_desc": "date DESC",
            "date_asc": "date ASC",
            "size_desc": "file_size DESC",
            "size_asc": "file_size ASC",
            "duration_desc": "duracion DESC",
        }
        order_by = sort_map.get(sort_order, "date DESC")

        # --- QUERY PRINCIPAL ---
        sql = f"""
            SELECT 
                id, message_id, chat_id, file_unique_id, 
                file_name, file_size, duration, 
                thumb_path, has_thumb, date, width, height, 
                caption, es_vertical, watch_later
            FROM videos_telegram
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        
        # --- QUERY CONTEO TOTAL (Para paginación) ---
        sql_count = f"SELECT COUNT(*) FROM videos_telegram WHERE {where_sql}"

        # Ejecutar conteo
        async with db.execute(sql_count, params) as cursor:
            total = (await cursor.fetchone())[0]

        # Ejecutar selección (añadimos limit/offset a los params)
        query_params = params + [limit, offset]
        async with db.execute(sql, query_params) as cursor:
            rows = await cursor.fetchall()
            
        videos = []
        for r in rows:
            v = dict(r)
            # Formatear duración para la vista
            d = v.get("duration", 0) or 0
            h, rem = divmod(d, 3600)
            m, s = divmod(rem, 60)
            if h > 0:
                v["duration_text"] = f"{h}:{m:02}:{s:02}"
            else:
                v["duration_text"] = f"{m}:{s:02}"
            
            # Formatear tamaño
            sz = v.get("file_size", 0) or 0
            for unit in ['B', 'KB', 'MB', 'GB']:
                if sz < 1024:
                    v["size_text"] = f"{sz:.1f} {unit}"
                    break
                sz /= 1024
            
            videos.append(v)

        return videos, total