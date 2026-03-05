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
from antares.data_collection.thermal.parsing.specific_param import ThermalSpecificParamParser
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # TODO tests are temporary (not pands df is returned by build_specific_param()
    # Build a thermal installed power file
    op_stat_filter = ["Available on market", "Inelastic supply / fixed profile"]
    parser = ThermalSpecificParamParser(RESOURCE_PATH, tmp_path, op_stat_filter, main_params, [2030])
    df_temp = parser.build_specific_param()

    assert isinstance(df_temp, pd.DataFrame)

    # # Asserts the file is created
    # generated_file_path = tmp_path / SPECIFIC_PARAM_FOLDER / "specific_param.xlsx"
    # assert generated_file_path.exists()
    # generated_df = pd.read_excel(generated_file_path)
    #
    # # Compare its content with the expected one
    # expected_file_path = RESOURCE_PATH / "expected_output_files" / "thermal_installed_power.xlsx"
    # expected_df = pd.read_excel(expected_file_path)
    # pd.testing.assert_frame_equal(generated_df, expected_df, check_dtype=False)
