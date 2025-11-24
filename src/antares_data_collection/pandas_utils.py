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


def add_total_column(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
    """
    Ajoute une colonne 'total' = col1 + col2.
    """
    df = df.copy()
    df["total"] = df[col1] + df[col2]
    return df
