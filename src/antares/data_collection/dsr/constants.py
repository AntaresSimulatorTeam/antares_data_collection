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

# DSR CLUSTER
DSR_INPUT_FILE = "DSR.csv"
DSR_EXPORT_ROOT_DIR = Path("DSR")
DSR_CLUSTER_FOLDER = DSR_EXPORT_ROOT_DIR / "cluster"
DSR_NAME_FILE = "cluster_DSR.xlsx"


class InputDsrColumns(StrEnum):
    STUDY_SCENARIO = "STUDY_SCENARIO"
    ZONE = "ZONE"
    MARKET_NODE = "MARKET_NODE"
    COMMISSIONING_DATE = "COMMISSIONING_DATE"
    DECOMMISSIONING_DATE_EXPECTED = "DECOMMISSIONING_DATE_EXPECTED"
    OP_STAT = "OP_STAT"
    SECTOR = "SECTOR"
    NET_MAX_GEN_CAP = "NET_MAX_GEN_CAP"
    DSR_DERATING_CURVE_ID = "DSR_DERATING_CURVE_ID"
    DSR_TYPE = "DSR_TYPE"
    MAX_HOURS = "MAX_HOURS"
    ACT_PRICE_DA = "ACT_PRICE_DA"


class OutputDsrColumns(StrEnum):
    TO_USE = "ToUse"
    AREA = "Area"
    NAME = "Name"
    GROUP = "Group"
    CAPACITY = "Capacity"
    NB_HOUR_PER_DAY = "nb_hour_per_day"
    MAX_HOUR_PER_DAY = "max_hour_per_day"
    PRICE = "price"
    NB_UNITS = "nb_units"
    FO_RATE = "FO_rate"
    FO_DURATION = "FO_duration"
    MODULATION = "Modulation"


# default values for static output columns
DSR_GROUP = "DSR"
DSR_NB_HOUR_PER_DAY = 24
DSR_FO_RATE = 0
DSR_FO_DURATION = 1


# DSR CAPACITY MODULATION

DSR_DERATING_INDEX_NAME = "DSR Derating Index.csv"
DSR_DERATING_NAME = "DSR Derating.csv"
DSR_CAPACITY_MODULATION_NAME_FILE = "capacity_modulation_DSR.xlsx"
DSR_EXPORT_DATE_COLUMN = "date"
DSR_DATE_INT_REFERENCE = 2028
DSR_CAPACITY_MODULATION_FOLDER = DSR_EXPORT_ROOT_DIR / "capacity_modulation"


class InputDeratingIndexColumns(StrEnum):
    ZONE = "ZONE"
    ID = "ID"
    TARGET_YEAR = "TARGET_YEAR"
    CURVE_UID = "CURVE_UID"
