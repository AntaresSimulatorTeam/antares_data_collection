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

import pytest

import re

import pandas as pd

from antares.data_collection.utils import filter_df_values_based_on_op_stat


@pytest.fixture
def df_test_utils_filter() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "ID": [1, 2, 3],
            "LABEL": ["a", "b", "c"],
        }
    )

    return df


def test_filter_df_values_based_on_op_stat_column_not_found(df_test_utils_filter: pd.DataFrame) -> None:
    # given
    df = df_test_utils_filter

    # then
    colname_filter = "LEBAL"
    with pytest.raises(ValueError, match=f"Column {colname_filter} not found in the dataframe"):
        filter_df_values_based_on_op_stat(["a"], df, colname_filter)


def test_filter_df_values_based_on_op_stat_empty_df(df_test_utils_filter: pd.DataFrame) -> None:
    # given
    df = df_test_utils_filter

    # then
    colname_filter = "LABEL"
    list_value_filter = ["d"]
    with pytest.raises(
        ValueError, match=re.escape(f"The given op_stat values {list_value_filter} are not present in the dataframe")
    ):
        filter_df_values_based_on_op_stat(list_value_filter, df, colname_filter)
