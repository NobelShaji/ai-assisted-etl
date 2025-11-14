from pathlib import Path
import requests
import pandas as pd

RAW = Path("data/raw")

def download_file(url: str, out_path: Path) -> Path:
    """Download a remote file to data/raw (or given path)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    return out_path

def read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV into a pandas DataFrame."""
    return pd.read_csv(path)

def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    """Write a DataFrame to Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
