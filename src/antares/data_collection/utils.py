# Copyright (c) 2024, RTE (https://www.rte-france.com)
#
# See AUTHORS.txt
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# SPDX-License-Identifier: MPL-2.0
#
# This file is part of the Antares project.
from pathlib import Path

import pandas as pd
import polars as pl

MAX_DECIMAL_DIGITS = 3
ANTARES_NODE_NAME_COLUMN = "antares_node"


def write_csv_file(file_path: Path, df: pd.DataFrame) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    polars_df = pl.from_pandas(df)
    polars_df.write_csv(file_path, separator=",", float_precision=MAX_DECIMAL_DIGITS)


def filter_df_values_based_on_op_stat(
    filter_op_stat_values: list[str], df: pd.DataFrame, column_name: str
) -> pd.DataFrame:
    """We want to keep only the lines were the OP_STAT value matches the user given ones"""
    if not filter_op_stat_values:
        return df
    if column_name not in df.columns:
        raise ValueError(f"Column {column_name} not found in the dataframe")
    df = df[df[column_name].isin(filter_op_stat_values)]
    if df.empty:
        # We want to raise as soon as possible to have a clear error msg
        raise ValueError(f"The given op_stat values {filter_op_stat_values} are not present in the dataframe")
    return df
