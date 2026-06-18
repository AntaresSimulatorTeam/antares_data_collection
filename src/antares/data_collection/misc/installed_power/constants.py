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

from antares.data_collection.misc.constants import MISC_ROOT_FOLDER


class OutputMiscPowerColumns(StrEnum):
    TO_USE = "ToUse"
    AREA = "Area"
    GROUP = "Group"
    CLUSTER = "Cluster"
    CATEGORY = "Category"


MISC_CATEGORY_NAME = "power"

MISC_INSTALL_POWER_FOLDER = MISC_ROOT_FOLDER / "installed power"
MISC_INSTALL_POWER_NAME_FILE = "installedMisc_PEMMDB.xlsx"
