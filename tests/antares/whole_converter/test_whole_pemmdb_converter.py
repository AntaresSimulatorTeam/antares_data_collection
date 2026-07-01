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

    # Check the output folders
    assert output_folder.exists()

    folders = [folder.name for folder in output_folder.iterdir()]
    assert sorted(folders) == ["DSR", "MISC", "ST_Storage", "link", "thermal"]

    dsr_folder = output_folder / "DSR"
    assert (dsr_folder / "capacity_modulation").is_dir()
    assert (dsr_folder / "cluster").is_dir()

    assert (output_folder / "link" / "PEMMDB_LINK.xlsx").exists()

    misc_folder = output_folder / "MISC"
    assert (misc_folder / "installed power").is_dir()
    assert (misc_folder / "load factor").is_dir()

    assert (output_folder / "ST_Storage" / "battery" / "clusters" / "cluster_battery_PEMMDB.xlsx").exists()

    thermal_folder = output_folder / "thermal"
    assert (thermal_folder / "installed power").is_dir()
    assert (thermal_folder / "technical parameters").is_dir()
