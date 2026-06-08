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

SPECIFIC_PARAM_FOLDER = Path("thermal") / "technical parameters"
SPECIFIC_PARAM_NAME_FILE = "specific_param_PEMMDB.xlsx"


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
    CLUSTER_PEMMDB = "cluster_PEMMDB"
    CLUSTER = "Cluster"
    MIN_STABLE_GEN = "min_stable_generation"
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
    NPO_MAX_WINTER = "NPO_max_winter"
    NPO_MAX_SUMMER = "NPO_max_summer"
    NB_UNIT = "nb_unit"


F_COLUMNS = [f"F{i}" for i in range(1, 13)]
P_COLUMNS = [f"P{i}" for i in range(1, 13)]
P_COLUMNS_WINTER = [f"P{i}" for i in [1, 2, 3, 10, 11, 12]]

# special header for output
DONNEES_PRINCIPALES = "Données principales"
EMPTY = None
PERCENT = "%"
RATIO = "0/1"
DAY = "day"
EURO_MWATT = "Euro/MWh"
NB = "nb"
FO_RATE_1 = "FO_Rate_1"
P_1 = "P_1"

header_before_header = {
    OutputThermalSpecificColumns.NODE: [DONNEES_PRINCIPALES, EMPTY],
    OutputThermalSpecificColumns.CLUSTER_PEMMDB: [EMPTY, EMPTY],
    OutputThermalSpecificColumns.CLUSTER: [EMPTY, EMPTY],
    OutputThermalSpecificColumns.MIN_STABLE_GEN: [EMPTY, PERCENT],
    OutputThermalSpecificColumns.SPINNING: [EMPTY, RATIO],
    OutputThermalSpecificColumns.EFFICIENCY: [EMPTY, RATIO],
    OutputThermalSpecificColumns.FO_RATE: [EMPTY, RATIO],
    OutputThermalSpecificColumns.FO_DURATION: [EMPTY, DAY],
    OutputThermalSpecificColumns.PO_DURATION: [EMPTY, DAY],
    OutputThermalSpecificColumns.PO_WINTER: [EMPTY, RATIO],
    OutputThermalSpecificColumns.MARGINAL_COST: [EMPTY, EURO_MWATT],
    OutputThermalSpecificColumns.MARKET_BID: [EMPTY, EURO_MWATT],
    OutputThermalSpecificColumns.MR_SPECIFIC: [EMPTY, RATIO],
    OutputThermalSpecificColumns.CM_SPECIFIC: [EMPTY, RATIO],
    OutputThermalSpecificColumns.NPO_MAX_WINTER: [EMPTY, EMPTY],
    OutputThermalSpecificColumns.NPO_MAX_SUMMER: [EMPTY, EMPTY],
    OutputThermalSpecificColumns.NB_UNIT: [EMPTY, NB],
    F_COLUMNS[0]: [FO_RATE_1, EMPTY],
    P_COLUMNS[0]: [P_1, EMPTY],
}
