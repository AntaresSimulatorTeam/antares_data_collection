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

from antares.data_collection.misc.installed_power.constants import MISC_ROOT_FOLDER

LOAD_FACTOR_FILE_INDEX_NAME = "Other RES Hourly Index.csv"
LOAD_FACTOR_FILE_TS_NAME = "Other RES Hourly.csv"


class InputLoadFactorIndexColumns(StrEnum):
    ZONE = "ZONE"
    ID = "ID"
    TARGET_YEAR = "TARGET_YEAR"
    CURVE_UID = "CURVE_UID"


EXPORT_DATE_COLUMN = "date"

MISC_LOAD_FACTOR_FOLDER = MISC_ROOT_FOLDER / "load factor" / "PEMMDB"
