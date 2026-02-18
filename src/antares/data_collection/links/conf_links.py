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
from typing import Any


class LinksFileConfig:
    def __init__(self) -> None:
        self.NTC_INDEX = "NTCs Index.csv"
        self.NTC_TS = "NTCs.csv"
        self.TRANSFER_LINKS = "Transfer Links.csv"

    def all_names(self) -> list[str]:
        return [self.NTC_INDEX, self.NTC_TS, self.TRANSFER_LINKS]


# structure of data files
class StrEnum(str, Enum):
    pass


# data "Transfer Links.csv"
class TransferLinks(StrEnum):
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
class NTCsIndex(StrEnum):
    CURVE_UID = "CURVE_UID"
    ZONE = "ZONE"
    ID = "ID"
    LABEL = "LABEL"
    COUNT = "COUNT"


# "NTCs.csv"
class NTCS(StrEnum):
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


# list of parameters for exports
class LinksExportParameters(Enum):
    HURDLE_COSTS = ("Hurdle Costs", 0.5)

    def __init__(self, label: str, default: Any):
        self.label = label
        self.default = default
