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

from antares.data_collection import parse_main_params
from antares.data_collection.batteries.constants import BATTERIES_FOLDER
from antares.data_collection.batteries.parsing import BatteriesParser
from tests.conftest import RESOURCE_PATH


def test_parse_main_params_real_test_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build a DSR cluster capacity file
    parser = BatteriesParser(
        RESOURCE_PATH,
        tmp_path,
        main_params,
        [2030, 2035],
    )

    start = time.time()
    parser.build_batteries()
    end = time.time()
    print("Duration BATTERIES", end - start)

    # Asserts the file is created
    generated_file_path = tmp_path / BATTERIES_FOLDER / "cluster_battery_format_pegase.xlsx"
    assert generated_file_path.exists()

    # read Excel workbook, one sheet by year
    file_wb = pd.ExcelFile(generated_file_path)
    sheet_names = file_wb.sheet_names

    generated_df = pd.read_excel(generated_file_path, sheet_name=sheet_names)
    assert list(generated_df.keys()) == ["2030", "2035"]

    # Compare its content with the expected one for any sheet
    expected_wb_file_path = RESOURCE_PATH / "expected_output_files" / "batteries" / "cluster_battery_format_pegase.xlsx"
    expected_wb = pd.ExcelFile(expected_wb_file_path)
    # 2030
    sheet_name = list(generated_df.keys())[0]
    expected_df_2030 = pd.read_excel(expected_wb, sheet_name=sheet_name)
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2030, check_dtype=False)
    # 2035
    sheet_name = list(generated_df.keys())[1]
    expected_df_2035 = pd.read_excel(expected_wb, sheet_name=sheet_name)
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2035, check_dtype=False)
