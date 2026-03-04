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
from pathlib import Path

import pandas as pd

from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.constants import (
    THERMAL_INSTALL_POWER_FOLDER,
)
from antares.data_collection.thermal.parsing.installed_power import ThermalInstallerPowerParser
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build for Chriselle
    parser = ThermalInstallerPowerParser(RESOURCE_PATH, tmp_path, ["Available on market"], main_params, [2040, 2050, 2060, 2027])
    parser.build_thermal_installed_power()
    print("ok")