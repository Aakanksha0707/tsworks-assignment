from pathlib import Path
from src.db import apply_schema
from src.config import get_database_url
if __name__ == "__main__":
    print(f"Using DATABASE_URL={get_database_url()}")
    schema = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
    apply_schema(schema)
    print("Schema applied successfully.")
