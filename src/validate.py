import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.dtypes import Float


# Minimal schema: only numeric checks on two key columns
trips_schema = DataFrameSchema(
    {
        "passenger_count": Column(Float, nullable=True),
        "total_amount": Column(Float, nullable=False),
    },
    coerce=True,
)


def validate_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a subset of columns, but return the full dataframe.

    - Runs pandera checks only on passenger_count + total_amount
    - Replaces those columns with the validated versions
    - Leaves all other columns exactly as they came from transform()
    """
    # Columns that exist in both schema + dataframe
    cols = [c for c in trips_schema.columns.keys() if c in df.columns]

    if not cols:
        # Nothing to validate, just return as is
        return df

    # Validate subset
    subset = df[cols].copy()
    validated_subset = trips_schema.validate(subset)

    # Put validated columns back into a copy of the original df
    df_valid = df.copy()
    for col in cols:
        df_valid[col] = validated_subset[col]

    return df_valid

