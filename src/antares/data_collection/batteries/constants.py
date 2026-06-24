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

# Global
BATTERIES_INPUT_FILE = "Batteries.csv"


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
