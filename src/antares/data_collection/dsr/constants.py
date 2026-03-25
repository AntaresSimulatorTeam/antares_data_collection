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

DSR_INPUT_FILE = "DSR.csv"


class InputDsrColumns(StrEnum):
    STUDY_SCENARIO = "STUDY_SCENARIO"
    ZONE = "ZONE"
    MARKET_NODE = "MARKET_NODE"
    COMMISSIONING_DATE = "COMMISSIONING_DATE"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    NET_MAX_GEN_CAP = "NET_MAX_GEN_CAP"
    DSR_DERATING_CURVE_ID = "DSR_DERATING_CURVE_ID"
    DSR_TYPE = "DSR_TYPE"
    MAX_HOURS = "MAX_HOURS"
    ACT_PRICE_DA = "ACT_PRICE_DA"
