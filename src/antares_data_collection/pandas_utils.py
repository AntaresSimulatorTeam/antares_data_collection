import pandas as pd


def add_total_column(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
    """
    Ajoute une colonne 'total' = col1 + col2.
    """
    df = df.copy()
    df["total"] = df[col1] + df[col2]
    return df
