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

NTC_FILE_NAME = "NTCs.csv"
NTC_INDEX_FILE_NAME = "NTCs Index.csv"
TRANSFER_LINKS_FILE_NAME = "Transfer Links.csv"


class NtcIndexColumns(StrEnum):
    CURVE_UID = "CURVE_UID"
    ID = "ID"
