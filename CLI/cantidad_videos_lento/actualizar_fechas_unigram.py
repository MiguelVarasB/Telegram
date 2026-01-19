import asyncio
import sys
from pathlib import Path
from typing import Iterable

import aiosqlite

# Permitir imports del proyecto (raíz /Telegram)
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.unigram import obtener_fechas_y_ids
from config import DB_PATH
from utils import log_timing

"""
Script utilitario: completa last_message_date en la base principal usando Unigram.

Flujo:
- Lee los canales de la tabla `chats` que no tienen last_message_date.
- Consulta la DB real de Unigram para obtener la fecha del último mensaje.
- Actualiza last_message_date solo cuando hay una fecha válida (no "N/A").
"""


async def _obtener_canales_sin_fecha() -> list[int]:
    query = """
        SELECT chat_id
        FROM chats
        WHERE (last_message_date IS NULL OR last_message_date = '')
        and activo = 1
        
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query) as cursor:
            rows = await cursor.fetchall()
            return [int(r[0]) for r in rows]


async def _listar_pendientes_detalle() -> list[tuple[int, str | None, str | None, str | None]]:
    """Devuelve [(chat_id, name, type, username)] aún sin fecha y activos."""
    query = """
        SELECT chat_id, name, type, username
        FROM chats
        WHERE (last_message_date IS NULL OR last_message_date = '')
          AND activo = 1
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query) as cursor:
            return [(int(r[0]), r[1], r[2], r[3]) for r in await cursor.fetchall()]


def _filtrar_fechas_validas(ids: Iterable[int]) -> dict[int, str]:
    uni_map = obtener_fechas_y_ids(ids)
    resultado: dict[int, str] = {}
    if not isinstance(uni_map, dict):
        return resultado

    for cid in ids:
        data = uni_map.get(int(cid))
        fecha = data.get("fecha") if isinstance(data, dict) else None
        if fecha and fecha != "N/A":
            resultado[int(cid)] = fecha
    return resultado


async def _aplicar_fechas(fechas: dict[int, str]) -> int:
    if not fechas:
        return 0
    datos = [(fecha, cid) for cid, fecha in fechas.items()]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            "UPDATE chats SET last_message_date = ? WHERE chat_id = ?",
            datos,
        )
        await db.commit()
    return len(datos)


async def actualizar_fechas_unigram() -> None:
    pendientes = await _obtener_canales_sin_fecha()
    log_timing(f"Canales sin fecha: {len(pendientes)}")
    if not pendientes:
        return

    fechas_validas = _filtrar_fechas_validas(pendientes)
    actualizados = await _aplicar_fechas(fechas_validas)

    log_timing(
        f"Actualizados {actualizados} canales con fecha desde Unigram. "
        f"Sin dato: {len(pendientes) - actualizados}"
    )

    # Listar los que siguen sin fecha (activos)
    pendientes_detalle = await _listar_pendientes_detalle()
    if pendientes_detalle:
        log_timing("Pendientes sin fecha (activos):")
        for cid, name, ctype, username in pendientes_detalle:
            etiqueta = username or ""
            log_timing(f"- {cid} | {name or 'Sin Nombre'} | {ctype or '?'} | {etiqueta}")
    else:
        log_timing("No quedan pendientes sin fecha (activos).")


if __name__ == "__main__":
    asyncio.run(actualizar_fechas_unigram())
