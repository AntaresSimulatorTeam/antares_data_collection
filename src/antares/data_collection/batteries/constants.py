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

# Input
BATTERIES_INPUT_FILE = "Batteries.csv"

# Output
BATTERIES_FOLDER = Path("Battery")

# Default values parameters
PEMMDB_PLANT_TYPE_MARKET = ["Battery utility scale"]
OP_STAT_MARKET = ["Available on market"]
PEMMDB_PLANT_TYPE_RESIDENTIAL = ["Battery residential"]
OP_STAT_RESIDENTIAL = ["Available on market", "Out of market - for PV/battery dispatch optimization"]
EFFICIENCY_INJECTION = 0.8996

# Output values parameters
GROUP_VALUES = ["battery", "battery_out"]


class InputBatteriesColumns(StrEnum):
    ZONE = "ZONE"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    MARKET_NODE = "MARKET_NODE"
    COMMISSIONING_DATE = "COMMISSIONING_DATE"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    PEMMDB_PLANT_TYPE = "PEMMDB_PLANT_TYPE"
    NET_MAX_CAP_GEN = "NET_MAX_CAP_GEN"
    NET_MAX_CAP_DEM = "NET_MAX_CAP_DEM"
    STO_CAP = "STO_CAP"


class OutputBatteriesColumns(StrEnum):
    AREA = "Area"
    NAME = "Name"
    GROUP = "Group"
    INJECTION = "Injection"
    WITHDRAWAL = "Withdrawal"
    STORAGE = "Storage"
    EFFICIENCY_INJECTION = "Efficiency_injection"
    EFFICIENCY_WITHDRAWAL = "Efficiency_withdrawal"
    INITIAL_LEVEL = "Initial_level"
    INITIAL_LEVEL_OPTIM = "Initial_level_optim"
    ENABLED = "Enabled"
    SERIES = "Series"
    CONSTRAINTS = "Constraints"


# default values for static output columns
DEFAULT_NAME = "battery_residential"
DEFAULT_EFFICIENCY_WITHDRAWAL = 1
DEFAULT_INITIAL_LEVEL = 0.5
DEFAULT_INITIAL_LEVEL_OPTIM = False
DEFAULT_SERIES = False
DEFAULT_CONSTRAINTS = False
