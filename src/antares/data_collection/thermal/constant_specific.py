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


class InputThermalColumns(StrEnum):
    STUDY_SCENARIO = "STUDY_SCENARIO"
    MARKET_NODE = "MARKET_NODE"
    COMMISSIONING_DATE = "COMMISSIONING_DATE"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    SCND_FUEL = "SCND_FUEL"
    SCND_FUEL_RT = "SCND_FUEL_RT"
    NET_MAX_GEN_CAP = "NET_MAX_GEN_CAP"
    PEMMDB_TECHNOLOGY = "PEMMDB_TECHNOLOGY"

    # added for 'specif param' part
    STD_EFF_NCV = "STD_EFF_NCV"
    FORCED_OUTAGE_RATE = "FORCED_OUTAGE_RATE"
    MEAN_TIME_REPAIR = "MEAN_TIME_REPAIR"
    PLAN_OUTAGE_ANNUAL_DAYS = "PLAN_OUTAGE_ANNUAL_DAYS"
    PLAN_OUTAGE_WINTER = "PLAN_OUTAGE_WINTER"
    NET_MIN_STAB_GEN = "NET_MIN_STAB_GEN"

    GRP_MRUN_CURVE_ID = "GRP_MRUN_CURVE_ID"
    GEN_UNT_MRUN_CURVE_ID = "GEN_UNT_MRUN_CURVE_ID"
    GEN_UNT_INELASTIC_ID = "GEN_UNT_INELASTIC_ID"
    GEN_UNT_D_CURVE_ID = "GEN_UNT_D_CURVE_ID"
    GRP_D_CURVE_ID = "GRP_D_CURVE_ID"


class OutputThermalSpecificColumns(StrEnum):
    NODE = "node"
    CLUSTER = "Cluster"
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