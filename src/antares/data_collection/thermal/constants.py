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

import pandas as pd

THERMAL_INPUT_FILE = "Thermal.csv"
THERMAL_INSTALL_POWER_FOLDER = Path("thermal") / "installed power"
THERMAL_INSTALL_POWER_FILE_FORMAT = "thermal_{trajectory}/{scenario}.xlsx"
DEFAULT_DECOMMISSIONING_DATE = pd.Timestamp(year=2100, month=1, day=1)
BIOMASS_SNCD_FUEL_VALUE = "Bio"
BIOMASS_CLUSTER_SUFFIX = "bio"
FUEL_MAPPING = {BIOMASS_CLUSTER_SUFFIX: "Mixed fuel", "virtuel": "Other"}


def get_starting_and_ending_timestamps_for_outputs(year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Implicit rule: For a given year, we have to consider the year starts in July of the previous year
    and ends in June of the current year.
    Example: 2030 : 1st July 2029 -> 30 June 2030
    """
    return pd.Timestamp(year - 1, 7, 1), pd.Timestamp(year, 6, 30)


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


class OutputThermalInstallPowerColumns(StrEnum):
    TO_USE = "ToUse"
    AREA = "Area"
    FUEL = "Fuel"
    TECHNOLOGY = "Technology"
    CLUSTER = "Cluster"
    CATEGORY = "Category"
