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

DSR_CLUSTER_FOLDER = DSR_EXPORT_ROOT_DIR / "cluster"
DSR_NAME_FILE = "cluster_DSR.xlsx"


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
