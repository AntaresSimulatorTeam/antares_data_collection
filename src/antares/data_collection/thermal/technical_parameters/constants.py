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

# Output constants

FOLDER_NAME = "PEMMDB"
TECHNICAL_PARAMS_FOLDER = Path("thermal") / "technical parameters" / "param_modulation" / FOLDER_NAME
CAPACITY_MODULATION_NAME = f"CM_{FOLDER_NAME}"
MUST_RUN_NAME = f"MR_{FOLDER_NAME}"

# Input constants

INELASTIC_INDEX_NAME = "Inelastic Index.csv"
INELASTIC_NAME = "Inelastic.csv"


class InputInelasticIndexColumns(StrEnum):
    ZONE = "ZONE"
    ID = "ID"
    TARGET_YEAR = "TARGET_YEAR"
    CURVE_UID = "CURVE_UID"
