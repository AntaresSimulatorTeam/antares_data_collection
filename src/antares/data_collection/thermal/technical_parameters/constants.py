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

##########
# Output Constants
##########

FOLDER_NAME = "PEMMDB"
TECHNICAL_PARAMS_FOLDER = Path("thermal") / "technical parameters" / "param_modulation" / FOLDER_NAME
CAPACITY_MODULATION_NAME = f"CM_{FOLDER_NAME}"
MUST_RUN_OUTPUT_NAME = f"MR_{FOLDER_NAME}"

##########
# Input Constants
##########

SCENARIO_TO_ALWAYS_CONSIDER = "All_years_ERAA_TYNDP"
GROUP_MUST_RUN_LABEL = "Must run ratio"

INELASTIC_INDEX_NAME = "Inelastic Index.csv"
MUST_RUN_INDEX_NAME = "Must-run Index.csv"
GROUP_MUST_RUN_INDEX_NAME = "Group Must-run Index.csv"
DERATING_INDEX_NAME = "Derating Index.csv"
GROUP_DERATING_INDEX_NAME = "Group Derating Index.csv"

INELASTIC_NAME = "Inelastic.csv"
MUST_RUN_NAME = "Must-run.csv"
GROUP_MUST_RUN_NAME = "Group Must-run.csv"
DERATING_NAME = "Derating.csv"
GROUP_DERATING_NAME = "Group Derating.csv"


class InputIndexColumns(StrEnum):
    ZONE = "ZONE"
    ID = "ID"
    TARGET_YEAR = "TARGET_YEAR"
    CURVE_UID = "CURVE_UID"


class InputGroupMustRunIndexColumns(StrEnum):
    ZONE = "ZONE"
    ID = "ID"
    TARGET_YEAR = "TARGET_YEAR"
    CURVE_UID = "CURVE_UID"
    LABEL = "LABEL"
