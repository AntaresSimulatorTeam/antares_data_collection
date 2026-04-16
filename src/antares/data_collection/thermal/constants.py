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

import pandas as pd

THERMAL_INPUT_FILE = "Thermal.csv"
DEFAULT_DECOMMISSIONING_DATE = pd.Timestamp(year=2100, month=1, day=1)
BIOMASS_SNCD_FUEL_VALUE = "Bio"
BIOMASS_CLUSTER_SUFFIX = "bio"
FUEL_MAPPING = {BIOMASS_CLUSTER_SUFFIX: "Mixed fuel", "virtuel": "Other"}


ANTARES_CLUSTER_NAME_COLUMN = "cluster_name"
ANTARES_NODE_NAME_COLUMN = "antares_node"


class InputThermalColumns(StrEnum):
    STUDY_SCENARIO = "STUDY_SCENARIO"
    ZONE = "ZONE"
    MARKET_NODE = "MARKET_NODE"
    COMMISSIONING_DATE = "COMMISSIONING_DATE"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    SCND_FUEL = "SCND_FUEL"
    SCND_FUEL_RT = "SCND_FUEL_RT"
    NET_MAX_GEN_CAP = "NET_MAX_GEN_CAP"
    PEMMDB_TECHNOLOGY = "PEMMDB_TECHNOLOGY"
    GRP_MRUN_CURVE_ID = "GRP_MRUN_CURVE_ID"
    GEN_UNT_MRUN_CURVE_ID = "GEN_UNT_MRUN_CURVE_ID"
    GRP_D_CURVE_ID = "GRP_D_CURVE_ID"
    GEN_UNT_D_CURVE_ID = "GEN_UNT_D_CURVE_ID"
    GEN_UNT_INELASTIC_ID = "GEN_UNT_INELASTIC_ID"
    # 'specif param' part
    STD_EFF_NCV = "STD_EFF_NCV"
    FORCED_OUTAGE_RATE = "FORCED_OUTAGE_RATE"
    MEAN_TIME_REPAIR = "MEAN_TIME_REPAIR"
    PLAN_OUTAGE_ANNUAL_DAYS = "PLAN_OUTAGE_ANNUAL_DAYS"
    PLAN_OUTAGE_WINTER = "PLAN_OUTAGE_WINTER"
    NET_MIN_STAB_GEN = "NET_MIN_STAB_GEN"


class OutputModulationColumns(StrEnum):
    HOUR = "heure"
    DATE = "DATE_HEURE"
