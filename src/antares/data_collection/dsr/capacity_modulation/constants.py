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

from antares.data_collection.dsr.constants import DSR_EXPORT_ROOT_DIR

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
