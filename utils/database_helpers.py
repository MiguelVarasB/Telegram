import aiosqlite
from typing import Optional


async def ensure_column(
    db: aiosqlite.Connection,
    table_name: str,
    column_name: str,
    column_type: str,
    default_value: Optional[str] = None
) -> bool:
    """
    Verifica si una columna existe en una tabla y la crea si no existe.
    
    Args:
        db: Conexión aiosqlite abierta
        table_name: Nombre de la tabla
        column_name: Nombre de la columna a verificar/crear
        column_type: Tipo SQL de la columna (ej: 'INTEGER', 'TEXT', 'INTEGER DEFAULT 0')
        default_value: Valor por defecto opcional (ej: 'NULL', '0', "'texto'")
    
    Returns:
        True si la columna ya existía, False si fue creada
    """
    async with db.execute(f"PRAGMA table_info({table_name})") as cursor:
        cols = [row[1] async for row in cursor]
    
    if column_name in cols:
        return True
    
    try:
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        if default_value is not None and "DEFAULT" not in column_type.upper():
            sql += f" DEFAULT {default_value}"
        
        await db.execute(sql)
        await db.commit()
        return False
    except Exception:
        pass
    
    return True


async def ensure_columns(
    db: aiosqlite.Connection,
    table_name: str,
    columns: list[tuple[str, str, Optional[str]]]
) -> dict[str, bool]:
    """
    Verifica y crea múltiples columnas en una tabla.
    
    Args:
        db: Conexión aiosqlite abierta
        table_name: Nombre de la tabla
        columns: Lista de tuplas (column_name, column_type, default_value)
    
    Returns:
        Dict con {column_name: existed} donde existed=True si ya existía
    """
    results = {}
    for col_name, col_type, default_val in columns:
        existed = await ensure_column(db, table_name, col_name, col_type, default_val)
        results[col_name] = existed
    return results


async def table_exists(db: aiosqlite.Connection, table_name: str) -> bool:
    """Verifica si una tabla existe en la base de datos."""
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ) as cursor:
        row = await cursor.fetchone()
        return row is not None
