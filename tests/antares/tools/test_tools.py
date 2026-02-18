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

import pandas as pd
from antares.data_collection.tools.tools import (
    thermal_filter_active_years_commissioning,
    thermal_year_to_overlapping_datetime,
)


def test_thermal_filter_active_years_commissioning() -> None:
    # given
    df_start = pd.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
    df_start_col = pd.to_datetime(df_start)

    df_end = pd.DataFrame({"year": [2017, 2020], "month": [2, 3], "day": [4, 5]})
    df_end_col = pd.to_datetime(df_end)

    df_input_data = pd.DataFrame({"start": df_start_col, "end": df_end_col})

    # when
    i_year = pd.to_datetime("2020-01-01")
    df_test = thermal_filter_active_years_commissioning(
        df_input=df_input_data,
        name_col_start_date="start",
        name_col_end_date="end",
        year_date=i_year,
    )

    assert df_test.shape == (1, 2)

    expected = df_input_data.iloc[[1]]
    pd.testing.assert_frame_equal(expected, df_test)


def test_thermal_year_to_overlapping_datetime() -> None:
    result = thermal_year_to_overlapping_datetime(2024)
    assert isinstance(result, pd.DatetimeIndex)
    assert result[0] == pd.Timestamp("2023-07-01")
    assert result[-1] == pd.Timestamp("2024-06-01")
    assert len(result) == 12
