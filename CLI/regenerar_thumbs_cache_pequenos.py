import os
import subprocess
from pathlib import Path
import sqlite3

from config import DB_PATH, THUMB_FOLDER
from unigram_cacheo.common import DB_LOCAL, CARPETAS, obtener_duracion_video

THRESHOLD_BYTES = 3 * 1024
NEW_HAS_THUMB = 3
SEEK_RATIO = 0.20  # 20%


def ffmpeg_thumb(video_path: Path, output_path: Path, seek_seconds: float, use_cuda: bool = True) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_cmd = [
        "-y",
        "-ss",
        f"{seek_seconds:.2f}",
        "-i",
        str(video_path),
        "-vframes",
        "1",
        "-vf",
        "scale=320:-1",
        "-q:v",
        "4",
        str(output_path),
    ]

    def _run(cmd):
        # text=False evita decodificación cp1252 y errores de Unicode
        res = subprocess.run(cmd, capture_output=True, text=False, timeout=20)
        return res.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0

    # Intento con GPU (NVDEC/NVENC) si se habilita
    if use_cuda:
        try:
            cmd_cuda = ["ffmpeg", "-hwaccel", "cuda", "-hwaccel_output_format", "cuda"] + base_cmd
            if _run(cmd_cuda):
                return True
        except Exception:
            pass

    # Fallback CPU
    try:
        cmd_cpu = ["ffmpeg"] + base_cmd
        return _run(cmd_cpu)
    except Exception:
        return False


def run():
    conn_local = sqlite3.connect(DB_LOCAL)
    conn_main = sqlite3.connect(DB_PATH)
    try:
        cur_main = conn_main.cursor()
        cur_local = conn_local.cursor()

        rows = cur_main.execute(
            """
            SELECT vt.chat_id, vt.message_id, vt.file_unique_id, COALESCE(vt.thumb_bytes, 0)
            FROM videos_telegram vt
            WHERE vt.file_unique_id IS NOT NULL
              AND vt.has_cache = 5
              AND (vt.thumb_bytes IS NULL OR vt.thumb_bytes < ?)
            """,
            (THRESHOLD_BYTES,),
        ).fetchall()

        total = len(rows)
        regenerados = 0
        no_archivo = 0
        fallos_ffmpeg = 0
        sin_cache = 0

        print(f"🔁 Regenerando thumbs para {total} videos con thumb <3KB o sin thumb_bytes...")

        for idx, (chat_id, msg_id, fuid, thumb_bytes) in enumerate(rows, start=1):
            if idx % 200 == 0 or idx == total:
                print(f"  Procesados {idx}/{total}")

            # Buscar archivo en cacheo por unique_id
            row_cache = cur_local.execute(
                "SELECT archivo FROM cacheo WHERE unique_id = ? AND tipo = 'video'",
                (fuid,),
            ).fetchone()
            if not row_cache:
                sin_cache += 1
                continue

            archivo = row_cache[0]
            video_path = Path(CARPETAS.get("video", "")) / archivo
            if not video_path.exists():
                no_archivo += 1
                continue

            dur = obtener_duracion_video(str(video_path)) or 0
            seek = max(0.0, dur * SEEK_RATIO)

            thumb_path = Path(THUMB_FOLDER) / str(chat_id) / f"{fuid}.webp"
            ok = ffmpeg_thumb(video_path, thumb_path, seek)
            if ok:
                size_bytes = thumb_path.stat().st_size
                cur_main.execute(
                    "UPDATE videos_telegram SET has_thumb = ?, thumb_bytes = ? WHERE chat_id = ? AND message_id = ?",
                    (NEW_HAS_THUMB, size_bytes, chat_id, msg_id),
                )
                regenerados += 1
            else:
                fallos_ffmpeg += 1

        conn_main.commit()

        print("\nResumen regeneración (thumbs en 20% del video):")
        print(f"  Total candidatos:    {total}")
        print(f"  Regenerados OK:      {regenerados}")
        print(f"  Sin archivo cache:   {sin_cache}")
        print(f"  Archivo faltante:    {no_archivo}")
        print(f"  Fallos ffmpeg:       {fallos_ffmpeg}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn_local.close()
        conn_main.close()


if __name__ == "__main__":
    run()
