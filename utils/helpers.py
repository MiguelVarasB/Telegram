"""
Funciones auxiliares y de conversión.
"""
import os
import datetime
from PIL import Image
from pyrogram.raw.types import InputPeerChannel, InputPeerChat, InputPeerUser, InputChannel, InputUser
from pyrogram.raw.functions.channels import GetChannels
from pyrogram.raw.functions.messages import GetChats
from pyrogram.raw.functions.users import GetUsers
from datetime import datetime

def log_timing(msg: str, end: str = "\n"):
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {msg}", end=end)

def obtener_id_limpio(peer) -> int | None:
    """Extrae el ID limpio de un peer de Telegram."""
    if isinstance(peer, InputPeerChannel):
        return int(f"-100{peer.channel_id}")
    elif isinstance(peer, InputPeerChat):
        return int(f"-{peer.chat_id}")
    elif isinstance(peer, InputPeerUser):
        return peer.user_id
    return None


def convertir_tamano(size_bytes: int | None) -> str:
    """Convierte bytes a formato legible (MB/GB)."""
    if not size_bytes:
        return "0 MB"
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def formatear_miles(value) -> str:
    try:
        if value is None:
            return "0"
        n = int(value)
        return f"{n:,}".replace(",", ".")
    except Exception:
        try:
            return str(value)
        except Exception:
            return "0"


def json_serial(obj):
    """Serializador JSON para tipos datetime."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def serialize_pyrogram(obj):
    """Serializa objetos Pyrogram a diccionarios/listas."""
    if hasattr(obj, "__dict__"):
        d = {}
        for k, v in obj.__dict__.items():
            if k.startswith("_"):
                continue
            d[k] = serialize_pyrogram(v)
        d["_type"] = obj.__class__.__name__
        return d
    elif isinstance(obj, list):
        return [serialize_pyrogram(x) for x in obj]
    elif isinstance(obj, bytes):
        return str(obj)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    else:
        return obj


def save_image_as_webp(source_path: str, dest_path: str) -> str:
    """Convierte una imagen temporal al formato WebP y la guarda en dest_path."""
    with Image.open(source_path) as img:
        if img.mode in ("RGBA", "LA"):
            converted = img.convert("RGBA")
        else:
            converted = img.convert("RGB")
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        converted.save(dest_path, format="WEBP", quality=90, method=6)
    return dest_path


async def force_resolve_peer(client, raw_peer):
    """Intenta 'despertar' al peer usando Raw API con tipos correctos."""
    try:
        if isinstance(raw_peer, InputPeerChannel):
            inp = InputChannel(channel_id=raw_peer.channel_id, access_hash=raw_peer.access_hash)
            await client.invoke(GetChannels(id=[inp]))
        elif isinstance(raw_peer, InputPeerChat):
            await client.invoke(GetChats(id=[raw_peer.chat_id]))
        elif isinstance(raw_peer, InputPeerUser):
            inp = InputUser(user_id=raw_peer.user_id, access_hash=raw_peer.access_hash)
            await client.invoke(GetUsers(id=[inp]))
    except Exception as e:
        print(f"⚠️ Force resolve failed: {e}")
