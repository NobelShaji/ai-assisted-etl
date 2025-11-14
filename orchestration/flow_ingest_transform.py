from prefect import flow, task, get_run_logger
from pathlib import Path
import pandas as pd

from src.warehouse import ensure_schemas, get_con
from src.ingest import download_file, read_csv, write_parquet
from src.llm_assist import suggest_cleaning_code
from src.validate import validate_trips


@task
def ingest_tlc_sample() -> Path:
    """Download a sample NYC taxi CSV into data/raw."""
    logger = get_run_logger()

    # Confirmed working CSV sample
    url = (
        "https://raw.githubusercontent.com/great-expectations/"
        "gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )

    out = Path("data/raw/taxi_sample.csv")
    download_file(url, out)

    logger.info(f"Downloaded {out}")
    return out



@task
def bronze_load(csv_path: Path) -> Path:
    """Load raw CSV into a bronze Parquet file."""
    logger = get_run_logger()
    df = read_csv(csv_path)
    bronze_path = Path("data/bronze/taxi_sample.parquet")
    write_parquet(df, bronze_path)
    logger.info(f"Wrote bronze parquet: {bronze_path}")
    return bronze_path


@task
def llm_transform(bronze_path: Path) -> pd.DataFrame:
    """
    Call the LLM-assist stub to generate cleaning code,
    then exec that code to transform the DataFrame.
    """
    logger = get_run_logger()
    df = pd.read_parquet(bronze_path)

    code = suggest_cleaning_code(
        {c: str(df[c].dtype) for c in df.columns},
        goal="clean text columns & ensure datetime for pickup/dropoff",
    )

    # IMPORTANT: use one shared dictionary for globals + locals
    scope = {}
    exec(code, scope, scope)
    transform = scope.get("transform")

    if transform is None:
        logger.warning("No transform(df) function found in generated code; using identity")
        transformed = df
    else:
        transformed = transform(df)

    logger.info("Applied LLM-suggested transform")
    return transformed



@task
def validate_and_write_silver(df: pd.DataFrame) -> Path:
    """Validate with pandera and write to data/silver."""
    logger = get_run_logger()

    # Ensure the key columns exist with expected names
    df = df.rename(
        columns={
            "tpep_pickup_datetime": "tpep_pickup_datetime",
            "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        }
    )

    df_valid = validate_trips(df)

    silver_path = Path("data/silver/taxi_sample_clean.parquet")
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    df_valid.to_parquet(silver_path, index=False)
    logger.info(f"Wrote silver parquet: {silver_path}")
    return silver_path


@task
def load_to_duckdb(silver_path: Path) -> None:
    """Create/replace silver.taxi_sample table in DuckDB warehouse."""
    logger = get_run_logger()
    ensure_schemas()
    con = get_con()
    con.execute(
        "CREATE OR REPLACE TABLE silver.taxi_sample AS "
        f"SELECT * FROM read_parquet('{silver_path.as_posix()}');"
    )
    logger.info("Loaded silver.taxi_sample into DuckDB")
    con.close()


@flow(name="ai-assisted-etl")
def main_flow():
    csv_path = ingest_tlc_sample()
    bronze_path = bronze_load(csv_path)
    transformed = llm_transform(bronze_path)
    silver_path = validate_and_write_silver(transformed)
    load_to_duckdb(silver_path)


if __name__ == "__main__":
    main_flow()
