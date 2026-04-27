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

from typing import TypeAlias

import pandas as pd

MAX_DECIMAL_DIGITS = 3
ANTARES_NODE_NAME_COLUMN = "antares_node"
DEFAULT_DECOMMISSIONING_DATE = pd.Timestamp(year=2100, month=1, day=1)
YearId: TypeAlias = int
SCENARIO_TO_ALWAYS_CONSIDER = "All_years_ERAA_TYNDP"
