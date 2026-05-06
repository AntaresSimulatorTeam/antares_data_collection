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

LINKS_NTC_INDEX_NAME = "NTCs Index.csv"
LINKS_NTC_TS_NAME = "NTCs.csv"
LINKS_TRANSFER_LINKS_NAME = "Transfer Links.csv"

NTC_FILTER_STR_VALUE = "NTC"
CURVE_UID_SPLIT_SYMBOL = ":"


# data "Transfer Links.csv"
class InputTransferLinksColumns(StrEnum):
    ZONE = "ZONE"
    MARKET_ZONE_SOURCE = "MARKET_ZONE_SOURCE"
    MARKET_ZONE_DESTINATION = "MARKET_ZONE_DESTINATION"
    TRANSFER_TYPE = "TRANSFER_TYPE"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    YEAR_VALID_START = "YEAR_VALID_START"
    YEAR_VALID_END = "YEAR_VALID_END"
    TRANSFER_TECHNOLOGY = "TRANSFER_TECHNOLOGY"
    NTC_LIMIT_CAPACITY_STATIC = "NTC_LIMIT_CAPACITY_STATIC"
    NTC_CURVE_ID = "NTC_CURVE_ID"
    NO_POLES = "NO_POLES"
    FOR = "FOR"


# "NTCs Index.csv"
class InputNTCsIndexColumns(StrEnum):
    CURVE_UID = "CURVE_UID"
    ZONE = "ZONE"
    ID = "ID"


# "NTCs.csv"
class InputNTCsColumns(StrEnum):
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"


# columns for export links file
class ExportLinksColumnsNames(StrEnum):
    NAME = "Name"

    WINTER_HP_DIRECT_MW = "Winter_HP_Direct_MW"
    WINTER_HP_INDIRECT_MW = "Winter_HP_Indirect_MW"
    WINTER_HC_DIRECT_MW = "Winter_HC_Direct_MW"
    WINTER_HC_INDIRECT_MW = "Winter_HC_Indirect_MW"

    SUMMER_HP_DIRECT_MW = "Summer_HP_Direct_MW"
    SUMMER_HP_INDIRECT_MW = "Summer_HP_Indirect_MW"
    SUMMER_HC_DIRECT_MW = "Summer_HC_Direct_MW"
    SUMMER_HC_INDIRECT_MW = "Summer_HC_Indirect_MW"

    FLOWBASED_PERIMETER = "Flowbased_perimeter"

    HVDC = "HVDC"

    SPECIFIC_TS = "Specific_TS"
    FORCED_OUTAGE_HVAC = "Forced_Outage_HVAC"
