"""Servicios utilitarios para leer la base cifrada de Unigram.

Expone ``obtener_fechas_y_ids`` para obtener, por diálogo, la fecha del
último mensaje y el id decodificado (message_id // 1048576).
"""
from __future__ import annotations

import os
import struct
from datetime import datetime
from typing import Dict, Iterable, Optional

from sqlcipher3 import dbapi2 as sqlite

# Valores por defecto (se pueden overridear vía argumentos o variables de entorno)
DEFAULT_MASTER_KEY = os.getenv(
    "UNIGRAM_MASTER_KEY",
    "f50f8b071d7eaf41c01e1a0309fdc01010c1247843a482c62577d325ab968f63",
)
DEFAULT_DB_PATH = os.getenv(
    "UNIGRAM_DB_PATH",
    r"C:\Users\TheMiguel\AppData\Local\Packages\38833FF26BA1D.UnigramPreview_g9c9v27vpyspw\LocalState\0\db.sqlite",
)


def _as_iterable_canales(canales: Optional[Iterable[int | str]]) -> Optional[list]:
    if canales is None:
        return None
    if isinstance(canales, (int, str)):
        return [canales]
    if isinstance(canales, Iterable):
        return list(canales)
    return None


def obtener_fechas_y_ids(
    canales: Optional[Iterable[int | str]] = None,
    *,
    db_path: Optional[str] = None,
    master_key: Optional[str] = None,
) -> Dict[int, Dict[str, object]]:
    """
    Retorna un diccionario ``{dialog_id: {"fecha": str, "id_mensaje_decodificado": int}}``.

    - ``canales`` puede ser un id único o iterable de ids (int/str) para filtrar.
    - ``db_path`` y ``master_key`` permiten overridear los valores por defecto.
    """

    ruta_db = db_path or DEFAULT_DB_PATH
    llave = master_key or DEFAULT_MASTER_KEY
    resultado: Dict[int, Dict[str, object]] = {}

    if not os.path.exists(ruta_db):
        return {"error": "DB no encontrada", "db_path": ruta_db}

    conn = None
    try:
        conn = sqlite.connect(ruta_db)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA key = \"x'{llave}'\";")
        cursor.execute("PRAGMA cipher_compatibility = 4;")

        canales_lista = _as_iterable_canales(canales)
        filtros = ""
        params = []
        if canales_lista:
            placeholders = ",".join(["?"] * len(canales_lista))
            filtros = f"WHERE dialog_id IN ({placeholders})"
            params.extend(canales_lista)

        # Traer el último message_id por dialog y su blob asociado usando join
        query = f"""
            WITH ultimos AS (
                SELECT dialog_id, MAX(message_id) AS max_mid
                FROM messages
                {filtros}
                GROUP BY dialog_id
            )
            SELECT m.dialog_id, m.message_id, m.data
            FROM messages m
            INNER JOIN ultimos u
              ON m.dialog_id = u.dialog_id AND m.message_id = u.max_mid
        """

        cursor.execute(query, params)
        for dialog_id, message_id, blob in cursor.fetchall():
            id_decodificado = message_id // 1048576

            fecha_humana = "N/A"
            if blob and len(blob) >= 32:
                try:
                    ts = struct.unpack("<I", blob[28:32])[0]
                    limite_superior = int(datetime.now().timestamp())  # hasta el año en curso
                    if 1356998400 <= ts <= limite_superior:  # desde 2013
                        fecha_humana = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

            resultado[int(dialog_id)] = {
                "fecha": fecha_humana,
                "id_mensaje_decodificado": id_decodificado,
            }
    except Exception as exc:
        resultado = {"error": str(exc), "db_path": ruta_db}
    finally:
        if conn is not None:
            conn.close()

    return resultado


__all__ = ["obtener_fechas_y_ids"]
