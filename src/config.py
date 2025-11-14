import os

# Path to our DuckDB warehouse file
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
