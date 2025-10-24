import os
from dotenv import load_dotenv
def load_env():
    load_dotenv(override=False)
def get_database_url(default: str = "sqlite:///movies.db") -> str:
    load_env()
    return os.getenv("DATABASE_URL", default)
