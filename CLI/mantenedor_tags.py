import asyncio
import argparse
import aiosqlite
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_PATH  # type: ignore
from utils import log_timing  # type: ignore


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tags (
    key TEXT PRIMARY KEY,
    name_en TEXT NOT NULL,
    name_es TEXT NOT NULL
)
"""


async def ensure_schema():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()


aSYNC_MESSAGES = {
    "upsert": "✅ Tag guardado",
    "list": "📋 Lista de tags",
}


async def upsert_tag(key: str, name_en: str, name_es: str):
    await ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO tags(key, name_en, name_es) VALUES(?, ?, ?)\n"
            "ON CONFLICT(key) DO UPDATE SET name_en=excluded.name_en, name_es=excluded.name_es",
            (key, name_en, name_es),
        )
        await db.commit()
    log_timing(f"✅ Tag guardado: {key} -> en='{name_en}' es='{name_es}'")


async def list_tags():
    await ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT key, name_en, name_es FROM tags ORDER BY key") as cur:
            rows = await cur.fetchall()
    if not rows:
        log_timing("ℹ️ No hay tags aún.")
        return
    log_timing("📋 Tags registrados:")
    for k, en, es in rows:
        print(f" - {k}: en='{en}' | es='{es}'")


async def main():
    parser = argparse.ArgumentParser(description="Mantenedor de Tags")
    sub = parser.add_subparsers(dest="cmd")

    set_cmd = sub.add_parser("set", help="Crear/actualizar un tag")
    set_cmd.add_argument("key", help="Clave única del tag")
    set_cmd.add_argument("name_en", help="Nombre en inglés")
    set_cmd.add_argument("name_es", help="Nombre en español")

    sub.add_parser("list", help="Listar todos los tags")

    args = parser.parse_args()

    if args.cmd == "set":
        await upsert_tag(args.key, args.name_en, args.name_es)
    elif args.cmd == "list":
        await list_tags()
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
