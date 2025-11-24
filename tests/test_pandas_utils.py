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
from antares_data_collection.pandas_utils import add_total_column


def test_add_total_column() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = add_total_column(df, "a", "b")

    assert "total" in result.columns
    assert result["total"].tolist() == [4, 6]
