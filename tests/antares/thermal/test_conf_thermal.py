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


from antares.data_collection.thermal.conf_thermal import ThermalLayout


def test_conf_thermal() -> None:
    # when
    infos_thermal = ThermalLayout()

    # then
    assert isinstance(infos_thermal, ThermalLayout)
    assert infos_thermal.input_data_name == "Thermal.csv"
    assert isinstance(infos_thermal.output_dir_thermal_country, Path)
    assert infos_thermal.output_dir_thermal_country == Path("thermal/country")
