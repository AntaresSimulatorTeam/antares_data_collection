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


from typing import Optional, List

import pandas as pd


# TODO add tests
def scenario_filter(
    df_input: pd.DataFrame, filter_params: Optional[List[str]] = None
) -> pd.DataFrame:
    valid_choices: List[str] = ["ERAA", "TYNDP"]

    # default: "ERAA"
    if filter_params is None or len(filter_params) != 1:
        filter_params = ["ERAA"]

    fp: str = filter_params[0]

    if fp not in valid_choices:
        raise ValueError(f"filter_params must be in {valid_choices}")

    filter_map: dict[str, str] = {
        "ERAA": r"ERAA&TYNDP|ERAA",
        "TYNDP": r"ERAA&TYNDP|TYNDP",
    }

    pattern: str = filter_map[fp]

    return df_input[df_input["STUDY_SCENARIO"].str.contains(pattern, regex=True)]
