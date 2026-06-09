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

from antares.data_collection.misc.installed_power.constants import MISC_INSTALL_POWER_FOLDER, OutputMiscPowerColumns
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
    parser.build_misc_installed_power_part()
    end = time.time()
    print("Duration Misc Installed power", end - start)

    # Asserts the file is created
    generated_file_path = tmp_path / MISC_INSTALL_POWER_FOLDER / "installedMisc_PEMMDB.xlsx"
    assert generated_file_path.exists()

    # read Excel workbook
    generated_df = pd.read_excel(generated_file_path)
    assert list(generated_df.columns) == [*[c.value for c in OutputMiscPowerColumns], 2030, 2035]

    # Compare its content with the expected
    expected_wb_file_path = RESOURCE_PATH / "expected_output_files" / "misc" / "installedMisc_PEMMDB.xlsx"
    expected_wb = pd.read_excel(expected_wb_file_path)
    pd.testing.assert_frame_equal(expected_wb, generated_df, check_dtype=False)
