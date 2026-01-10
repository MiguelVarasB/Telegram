import hashlib
import json
import os
import subprocess
from typing import Dict, Optional


VIDEO_EXTENSIONS = (".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm")


def get_video_metadata(file_path: str) -> Optional[Dict[str, str]]:
    """
    Extrae metadatos técnicos de un video usando ffprobe.
    Devuelve width, height, duration, nb_frames y size en bytes.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,duration,nb_frames",
        "-of",
        "json",
        file_path,
    ]

    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False
        )
        data = json.loads(result.stdout or "{}")

        if "streams" in data and len(data["streams"]) > 0:
            stream = data["streams"][0]
            return {
                "width": stream.get("width", 0),
                "height": stream.get("height", 0),
                "duration": stream.get("duration", "0"),
                "nb_frames": stream.get("nb_frames", "0"),
                "size": os.path.getsize(file_path),
            }
    except Exception as exc:  # pragma: no cover - logging simple
        print(f"Error al obtener metadatos de {file_path}: {exc}")
    return None


def generate_unique_hash(meta: Dict[str, str]) -> str:
    """
    Crea un hash corto (12 caracteres) a partir de los metadatos del video.
    """
    fingerprint = (
        f"{meta['size']}_{meta['duration']}_{meta['width']}x{meta['height']}_{meta['nb_frames']}"
    )
    full_hash = hashlib.sha256(fingerprint.encode()).hexdigest()
    return full_hash[:12]


def hash_video_file(file_path: str) -> Optional[str]:
    """
    Genera el hash único de un archivo de video. Devuelve None si falla.
    """
    meta = get_video_metadata(file_path)
    if not meta:
        return None
    return generate_unique_hash(meta)


def iter_hashed_videos(root_path: str):
    """
    Recorre recursivamente root_path y produce tuplas (path, hash).
    Solo procesa extensiones declaradas en VIDEO_EXTENSIONS.
    """
    if not os.path.isdir(root_path):
        raise ValueError(f"La ruta no es válida: {root_path}")

    for root, _dirs, files in os.walk(root_path):
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                full_path = os.path.join(root, file)
                video_hash = hash_video_file(full_path)
                yield full_path, video_hash
