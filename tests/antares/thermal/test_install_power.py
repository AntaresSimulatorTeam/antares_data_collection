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

from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.constants import (
    THERMAL_INSTALL_POWER_FOLDER,
)
from antares.data_collection.thermal.parsing.installed_power import ThermalInstallerPowerParser


def test_truc(tmp_path: Path) -> None:
    resource_path = Path("/home/belthlemar/Projects/Antares/antares_data_collection/tests/antares/resources")
    main_params = parse_main_params(resource_path / "MAIN_PARAMS_2025.xlsx")
    parser = ThermalInstallerPowerParser(resource_path, tmp_path, ["Available on market"], main_params, [2030])
    parser.build_thermal_installed_power()
    assert (tmp_path / THERMAL_INSTALL_POWER_FOLDER / "thermal_installed_power.xlsx").exists()
    print("ok")
