from pathlib import Path
import json
from datetime import datetime
import textwrap


def suggest_cleaning_code(schema: dict, goal: str) -> str:
    """
    Stub that emulates an LLM suggesting cleaning code.

    It logs the request to docs/llm_logs/ and returns a Python
    code string that defines a transform(df) function.

    In a real deployment, this is where you'd call an actual LLM
    (OpenAI, etc.) to generate transformation code based on the
    input schema + goal.
    """

    # 1) Log the "prompt" we would send to an LLM
    logs_dir = Path("docs/llm_logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"llm_suggestion_{ts}.json"
    log_path.write_text(
        json.dumps(
            {
                "schema": schema,
                "goal": goal,
                "note": "local stub only, no external LLM called",
            },
            indent=2,
        )
    )

    # 2) Return transform code as a string.
    #    IMPORTANT: we keep ALL columns and only clean a couple.
    code = textwrap.dedent(
        """
        import pandas as pd

        def transform(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()

            # Example of "AI-suggested" cleaning rules:
            # - Make sure passenger_count and total_amount are numeric
            # - Drop obviously bad total_amounts (negative)
            if "passenger_count" in df.columns:
                df["passenger_count"] = pd.to_numeric(
                    df["passenger_count"], errors="coerce"
                )

            if "total_amount" in df.columns:
                df["total_amount"] = pd.to_numeric(
                    df["total_amount"], errors="coerce"
                )
                df = df[df["total_amount"] >= 0]

            # NOTE: we are NOT dropping any other columns.
            # The rest of the taxi schema (pickup, dropoff, distance, etc.)
            # will flow through unchanged so we can query them in DuckDB.
            return df
        """
    )

    return code

