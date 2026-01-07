"""
Operaciones de base de datos para la tabla de tags.
"""
from typing import List, Dict

from .connection import get_db


async def db_ensure_tags_table():
    async with get_db() as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                key TEXT PRIMARY KEY,
                name_en TEXT NOT NULL,
                name_es TEXT NOT NULL
            )
            """
        )
        await db.commit()


async def db_upsert_tag(key: str, name_en: str, name_es: str) -> None:
    await db_ensure_tags_table()
    async with get_db() as db:
        await db.execute(
            """
            INSERT INTO tags(key, name_en, name_es)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE
            SET name_en=excluded.name_en,
                name_es=excluded.name_es
            """,
            (key, name_en, name_es),
        )
        await db.commit()


async def db_list_tags() -> List[Dict[str, str]]:
    await db_ensure_tags_table()
    async with get_db() as db:
        async with db.execute(
            "SELECT key, name_en, name_es FROM tags ORDER BY key"
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"key": k, "name_en": en, "name_es": es}
        for k, en, es in rows
    ]
