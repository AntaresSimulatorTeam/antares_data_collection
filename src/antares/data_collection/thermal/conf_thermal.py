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


class ThermalLayout:
    INPUT_DATA_STEM = "Thermal"
    INPUT_DATA_SUFFIX = ".csv"

    DIR_OUTPUT_THERMAL = "thermal"
    SUB_DIR_OUTPUT_THERMAL_COUNTRY = "country"
    SUB_DIR_OUTPUT_THERMAL_TECHNICAL = "technicalParameters"

    @property
    def input_data_name(self) -> str:
        return f"{self.INPUT_DATA_STEM}{self.INPUT_DATA_SUFFIX}"

    @property
    def output_dir_thermal_country(self) -> Path:
        return Path(self.DIR_OUTPUT_THERMAL) / self.SUB_DIR_OUTPUT_THERMAL_COUNTRY
