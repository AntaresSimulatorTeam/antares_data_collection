import pandas as pd
from antares_data_collection.pandas_utils import add_total_column


def test_add_total_column():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = add_total_column(df, "a", "b")

    assert "total" in result.columns
    assert result["total"].tolist() == [4, 6]
