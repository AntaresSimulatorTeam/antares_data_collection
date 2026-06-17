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
import pytest

import time

from pathlib import Path

import pandas as pd

from antares.data_collection import parse_main_params
from antares.data_collection.misc.load_factor.constants import MISC_LOAD_FACTOR_FOLDER
from antares.data_collection.misc.parsing import MiscParser
from tests.conftest import RESOURCE_PATH


@pytest.mark.parametrize(
    "pemmdb_plant_type, cluster_bp",
    [
        ("Marine", "wave"),
        ("Waste", "waste"),
        ("Small biomass", "biomass"),
        ("Geothermal", "geothermal"),
    ],
)
def test_nominal_case(tmp_path: Path, pemmdb_plant_type: str, cluster_bp: str) -> None:
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
    generated_folder_path = tmp_path / MISC_LOAD_FACTOR_FOLDER
    name_file = f"load_factor_{cluster_bp}_{2030 - 1}-{2030}.csv"
    load_factor_file_path = generated_folder_path / pemmdb_plant_type / cluster_bp / name_file

    assert load_factor_file_path.exists()

    generated_file = pd.read_csv(load_factor_file_path)

    # Compare their contents with the expected ones
    expected_folder_path = RESOURCE_PATH / "expected_output_files" / "misc"

    expected_load_factor_file = pd.read_csv(expected_folder_path / name_file)
    pd.testing.assert_frame_equal(generated_file, expected_load_factor_file, check_dtype=False)
