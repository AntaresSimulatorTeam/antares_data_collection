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

from antares.data_collection import PEMMDBConverter
from tests.conftest import RESOURCE_PATH


def test_whole_converter(tmp_path: Path) -> None:
    # Build the converter object
    main_params_path = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"
    input_folder = RESOURCE_PATH
    output_folder = tmp_path
    years = [2030, 2035]
    converter = PEMMDBConverter(input_folder, output_folder, main_params_path, years)
    # Thermals
    op_stat = ["Available on market", "Inelastic supply / fixed profile"]
    converter.build_thermal_files(op_stat)
    # Dsr
    converter.build_dsr_files(op_stat, ["Demand shedding", "Demand shifting"], [-1])
    # Batteries
    converter.build_batteries_files()
    # Links
    converter.build_link_files()
    # Misc
    converter.build_misc_files(op_stat)
