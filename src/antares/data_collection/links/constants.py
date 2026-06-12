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

LINKS_NTC_INDEX_NAME = "NTCs Index.csv"
LINKS_NTC_TS_NAME = "NTCs.csv"
LINKS_TRANSFER_LINKS_NAME = "Transfer Links.csv"

NTC_FILTER_STR_VALUE = "NTC"
CURVE_UID_SPLIT_SYMBOL = ":"
HVDC_NAME_TECHNOLOGY = "HVDC"

LINKS_CLUSTER_FOLDER = "link"
LINKS_OUTPUT_NAME_FILE = "PEMMDB_LINK.xlsx"

FIRST_SHEET_NAME = "parameters"

WINTER_SEASON = "winter"
SUMMER_SEASON = "summer"
HOUR_PEAK = "HP"
HOUR_OFFPEAK = "HC"


class Direction(StrEnum):
    DIRECT = "Direct"
    INDIRECT = "Indirect"


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


# Default value to fill for column "FOR" in "Transfer Links.csv"
FILL_FOR_VALUES = 0.05
MAX_DECIMAL_DIGITS_FOR = 2


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

    HVDC_DIRECT = "HVDC_MW_direct"
    HVDC_INDIRECT = "HVDC_MW_Indirect"
    HVDC_NB_DIRECT = "HVDC_Nb_Direct"
    HVDC_NB_INDIRECT = "HVDC_Nb_Indirect"
    HVDC_FOR_DIRECT = "HVDC_FO_Rate_direct"
    HVDC_FOR_INDIRECT = "HVDC_FO_Rate_indirect"


# The first tab in the export file is a data frame of constant parameters
DEFAULT_LINK_PARAMETERS = pd.DataFrame(
    data=[0.1, False],
    index=["Hurdle Costs", "HVDC"],
    columns=["value"],
)
