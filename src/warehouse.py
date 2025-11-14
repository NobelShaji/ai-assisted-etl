import duckdb
from src.config import DUCKDB_PATH

def get_con():
    """Return a DuckDB connection."""
    return duckdb.connect(DUCKDB_PATH)

def ensure_schemas():
    """Create bronze/silver/gold schemas if they don't exist."""
    con = get_con()
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.close()
