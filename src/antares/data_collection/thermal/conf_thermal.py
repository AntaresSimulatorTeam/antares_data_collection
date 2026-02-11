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
from enum import Enum
from pathlib import Path


# File names and dir structure
class ThermalLayout:
    INPUT_DATA_STEM = "Thermal"
    INPUT_DATA_SUFFIX = ".csv"

    DIR_OUTPUT_THERMAL = "thermal"
    SUB_DIR_OUTPUT_THERMAL_COUNTRY = "country"
    SUB_DIR_OUTPUT_THERMAL_TECHNICAL = "technicalParameters"

    @property
    def input_data_name(self) -> str:
        return f"{self.INPUT_DATA_STEM}{self.INPUT_DATA_SUFFIX}"

    @property
    def output_dir_thermal_country(self) -> Path:
        return Path(self.DIR_OUTPUT_THERMAL) / self.SUB_DIR_OUTPUT_THERMAL_COUNTRY

    @property
    def default_values_column_op_stat(self) -> list[str]:
        return ["Available on market", "Inelastic supply / fixed profile"]


# structure of data "Thermal.csv"
# only columns necessary for power/number processing
class ThermalDataColumns(Enum):
    ZONE = "ZONE"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    MARKET_NODE = "MARKET_NODE"
    DECOMMISSIONING_DATE_OFFICIAL = "DECOMMISSIONING_DATE_OFFICIAL"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    SCND_FUEL = "SCND_FUEL"
    SCND_FUEL_RT = "SCND_FUEL_RT"
    NET_MAX_GEN_CAP = "NET_MAX_GEN_CAP"
    PEMMDB_TECHNOLOGY = "PEMMDB_TECHNOLOGY"


# new columns computed
class ThermalComputedColumns(Enum):
    BIO_MAX_GENERATION_MW = "BIO_MAX_GENERATION_MW"
    FOSSIL_MAX_GENERATION_MW = "FOSSIL_MAX_GENERATION_MW"
