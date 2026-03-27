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

from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.param_modulation.constants import TECHNICAL_PARAMS_FOLDER
from antares.data_collection.thermal.parsing import ThermalParser
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build the thermal param modulation files
    parser = ThermalParser(
        RESOURCE_PATH, tmp_path, ["Available on market", "Inelastic supply / fixed profile"], main_params, [2030, 2035]
    )
    start = time.time()
    parser.build_param_modulation()
    end = time.time()
    print("Param modulation duration", end - start)

    # Asserts the files are created
    generated_folder_path = tmp_path / TECHNICAL_PARAMS_FOLDER
    cm_2030_path = generated_folder_path / "CM_PEMMDB_2029-2030.csv"
    cm_2035_path = generated_folder_path / "CM_PEMMDB_2034-2035.csv"
    mr_2030_path = generated_folder_path / "MR_PEMMDB_2029-2030.csv"
    mr_2035_path = generated_folder_path / "MR_PEMMDB_2034-2035.csv"
    assert cm_2030_path.exists()
    assert cm_2035_path.exists()
    assert mr_2030_path.exists()
    assert mr_2035_path.exists()

    generated_cm_2030 = pd.read_csv(cm_2030_path)
    generated_cm_2035 = pd.read_csv(cm_2035_path)
    generated_mr_2030 = pd.read_csv(mr_2030_path)
    generated_mr_2035 = pd.read_csv(mr_2035_path)

    # Compare their contents with the expected ones
    expected_folder_path = RESOURCE_PATH / "expected_output_files" / "thermal"

    expected_cm_2030 = pd.read_csv(expected_folder_path / "CM_PEMMDB_2029-2030.csv")
    pd.testing.assert_frame_equal(generated_cm_2030, expected_cm_2030, check_dtype=False)

    expected_cm_2035 = pd.read_csv(expected_folder_path / "CM_PEMMDB_2034-2035.csv")
    pd.testing.assert_frame_equal(generated_cm_2035, expected_cm_2035, check_dtype=False)

    expected_mr_2030 = pd.read_csv(expected_folder_path / "MR_PEMMDB_2029-2030.csv")
    pd.testing.assert_frame_equal(generated_mr_2030, expected_mr_2030, check_dtype=False)

    expected_mr_2035 = pd.read_csv(expected_folder_path / "MR_PEMMDB_2034-2035.csv")
    pd.testing.assert_frame_equal(generated_mr_2035, expected_mr_2035, check_dtype=False)
