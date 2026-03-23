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

from enum import StrEnum
from pathlib import Path

import numpy as np
import pandas as pd

SPECIFIC_PARAM_FOLDER = Path("thermal") / "technicalParameters"


def weighted_avg(df: pd.DataFrame, value_col: str, weight_col: str) -> float:
    """
    Compute weighted average for a given column from a Data Frame

    Args:
        df: Input pandas DataFrame containing the data.
        value_col: Name of the column containing the values to average.
        weight_col: Name of the column containing the weights associated
            with each value.

    Returns:
        The weighted average as a float.
    """
    return float(np.average(df[value_col], weights=df[weight_col]))


class OutputThermalSpecificColumns(StrEnum):
    NODE = "node"
    CLUSTER = "Cluster"
    NODE_ENTSOE = "node_ENTSOE"
    COMMENTS = "comments"
    CLUSTER_PEMMDB = "cluster_PEMMDB"
    MIN_STABLE_GEN = "min_stable_gen"
    SPINNING = "spinning"
    EFFICIENCY = "efficiency"
    FO_RATE = "FO_rate"
    FO_DURATION = "FO_duration"
    PO_DURATION = "PO_duration"
    PO_WINTER = "PO_winter"
    MARGINAL_COST = "marginal_cost"
    MARKET_BID = "market_bid"
    MR_SPECIFIC = "MR_specific"
    CM_SPECIFIC = "CM_specific"
    NB_UNIT = "nb_unit"


F_COLUMNS = [f"F{i}" for i in range(1, 13)]
P_COLUMNS = [f"P{i}" for i in range(1, 13)]
P_COLUMNS_WINTER = [f"P{i}" for i in [1, 2, 3, 10, 11, 12]]
