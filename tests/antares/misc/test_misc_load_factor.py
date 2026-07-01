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

import time

from pathlib import Path

import pandas as pd

from antares.data_collection.misc.load_factor.constants import MISC_LOAD_FACTOR_FOLDER
from antares.data_collection.misc.parsing import MiscParser
from antares.data_collection.referential_data.main_params import parse_main_params
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build a thermal installed power file
    parser = MiscParser(
        RESOURCE_PATH, tmp_path, ["Available on market", "Inelastic supply / fixed profile"], main_params, [2030, 2035]
    )

    start = time.time()
    parser.build_misc_load_factor_part()
    end = time.time()
    print("Duration Misc Load Factor", end - start)

    # Asserts the files are created
    list_folders_values = [
        ("wave", "Marine"),
        ("waste", "Waste"),
        ("biomass", "Small biomass"),
        ("geothermal", "Geothermal"),
    ]

    generated_folder_path = tmp_path / MISC_LOAD_FACTOR_FOLDER

    for year in [2030, 2035]:
        for tuple_values in list_folders_values:
            cluster_bp = tuple_values[0]
            pemmdb_plant_type = tuple_values[1]

            name_file = f"load_factor_{cluster_bp}_{year - 1}-{year}.csv"
            load_factor_file_path = generated_folder_path / cluster_bp / pemmdb_plant_type / name_file

            assert load_factor_file_path.exists()

            generated_file = pd.read_csv(load_factor_file_path)

            # Compare their contents with the expected ones
            expected_folder_path = RESOURCE_PATH / "expected_output_files" / "misc"

            expected_load_factor_file = pd.read_csv(expected_folder_path / name_file)
            pd.testing.assert_frame_equal(generated_file, expected_load_factor_file, check_dtype=False)
