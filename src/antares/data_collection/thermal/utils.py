from pathlib import Path

import pandas as pd


def parse_input_file(input_file_path: Path, expected_columns: list[str]) -> pd.DataFrame:
    if not input_file_path.exists():
        raise ValueError(f"File {input_file_path} not found")

    # Checks that all expected columns exist
    df = pd.read_csv(input_file_path)
    existing_cols = set(df.columns)
    for expected_column in expected_columns:
        if expected_column not in existing_cols:
            raise ValueError(f"Column {expected_column} not found in {input_file_path}")

    # Keep useful columns only
    return df[expected_columns]


def get_starting_and_ending_timestamps_for_outputs(year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Implicit rule: For a given year, we have to consider the year starts in July of the previous year
    and ends in June of the current year.
    Example: 2030 : 1st July 2029 -> 30 June 2030
    """
    return pd.Timestamp(year - 1, 7, 1), pd.Timestamp(year, 6, 30)
