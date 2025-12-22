"""
Gestor de caché en memoria RAM.
Ahora soporta el 'Pasaporte' (Objeto Mensaje) para evitar re-negociaciones con Telegram.
"""

# Estructura: 
# { 
#   "video_id": {
#       "data": bytes, 
#       "total_size": int, 
#       "mime_type": str,
#       "message": object  <-- ¡ESTO ES LO NUEVO!
#    } 
# }
_RAM_CACHE = {}

def store_in_ram(video_id: str, data: bytes, total_size: int, mime_type: str, message_obj=None):
    """
    Guarda el chunk, los metadatos y el objeto mensaje (pasaporte).
    """
    if not video_id:
        return
    
    # Si ya existe, actualizamos
    if video_id in _RAM_CACHE:
        entry = _RAM_CACHE[video_id]
        
        # Concatenamos datos si estamos añadiendo más buffer
        # (OJO: aquí asumimos que 'data' es el buffer acumulado completo, 
        # como lo hace tu prefetch.py actual. Si fuera solo un chunk nuevo, habría que hacer append).
        # Dado tu prefetch actual: buffer.extend(chunk) -> store_in_ram(..., bytes(buffer)...)
        # Significa que 'data' SIEMPRE trae todo lo acumulado.
        
        # Solo actualizamos si el nuevo buffer es más grande (progresivo)
        if len(data) > len(entry["data"]):
            entry["data"] = data
            
        # Si nos pasan el mensaje y no lo teníamos (o para actualizarlo), lo guardamos
        if message_obj:
            entry["message"] = message_obj

    else:
        # Entrada nueva
        _RAM_CACHE[video_id] = {
            "data": data,
            "total_size": total_size,
            "mime_type": mime_type,
            "message": message_obj
        }

def get_from_ram(video_id: str) -> dict | None:
    """Recupera el objeto completo de la caché."""
    return _RAM_CACHE.get(video_id)

def clear_ram_cache():
    """Limpia toda la caché."""
    count = len(_RAM_CACHE)
    _RAM_CACHE.clear()
    print(f"🧹 [RAM] Caché vaciada. {count} items eliminados.")

def get_ram_usage_count():
    """Retorna cuántos videos hay cacheados."""
    return len(_RAM_CACHE)