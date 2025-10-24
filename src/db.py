from pathlib import Path
from sqlalchemy import create_engine
from .config import get_database_url

def get_engine():
    return create_engine(get_database_url(), future=True)

def apply_schema(schema_path: Path):
    engine = get_engine()
    sql = schema_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)
