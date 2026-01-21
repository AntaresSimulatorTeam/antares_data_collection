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


class LinksFileConfig:
    def __init__(self) -> None:
        self.NTC_INDEX = "NTCs Index.csv"
        self.NTC_TS = "NTCs.csv"
        self.TRANSFER_LINKS = "Transfer Links.csv"

    def all_names(self) -> list[str]:
        return [self.NTC_INDEX, self.NTC_TS, self.TRANSFER_LINKS]


# structure Referential


# sheet names
class ReferentialSheetNames(Enum):
    PAYS = "PAYS"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    LINKS = "LINKS"
    PEAK_PARAMS = "PEAK_PARAMS"


# sheet "PAYS"
class CountryColumnsNames(Enum):
    NOM_PAYS = "Nom_pays"
    CODE_PAYS = "code_pays"
    AREAS = "areas"
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# sheet "STUDY_SCENARIO"
class StudyScenarioColumnsNames(Enum):
    YEAR = "YEAR"
    STUDY_SCENARIO = "STUDY_SCENARIO"


# sheet "LINKS"
class LinksColumnsNames(Enum):
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# "PEAK_PARAMS"
class PeakParamsColumnsNames(Enum):
    HOUR = "hour"
    PERIOD_HOUR = "period_hour"
    MONTH = "month"
    PERIOD_MONTH = "period_month"


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

    HVDC_DIRECT = "HVDC_Direct"
    HVDC_INDIRECT = "HVDC_Indirect"

    SPECIFIC_TS = "Specific_TS"
    FORCED_OUTAGE_HVAC = "Forced_Outage_HVAC"
