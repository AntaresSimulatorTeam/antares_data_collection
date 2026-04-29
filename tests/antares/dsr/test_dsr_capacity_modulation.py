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

from antares.data_collection.dsr.capacity_modulation.constants import DSR_CAPACITY_MODULATION_FOLDER
from antares.data_collection.dsr.parsing import DsrParser
from antares.data_collection.referential_data.main_params import parse_main_params
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build a DSR cluster capacity file
    parser = DsrParser(
        RESOURCE_PATH,
        tmp_path,
        ["Available on market", "Inelastic supply / fixed profile"],
        ["Demand shedding", "Demand shifting"],
        [-1],
        main_params,
        [2030, 2035],
    )

    start = time.time()
    parser.build_dsr_capacity_modulation_part()
    end = time.time()
    print("Duration DSR Capacity Modulation", end - start)

    # Asserts the file is created
    generated_file_path = tmp_path / DSR_CAPACITY_MODULATION_FOLDER / "capacity_modulation_DSR.xlsx"
    assert generated_file_path.exists()

    # read Excel workbook, one sheet by year
    file_wb = pd.ExcelFile(generated_file_path)
    sheet_names = file_wb.sheet_names

    generated_df = pd.read_excel(generated_file_path, sheet_name=sheet_names)
    assert list(generated_df.keys()) == ["2029-2030", "2034-2035"]

    # Compare its content with the expected one for any sheet
    expected_wb_file_path = RESOURCE_PATH / "expected_output_files" / "dsr" / "capacity_modulation_DSR.xlsx"
    expected_wb = pd.ExcelFile(expected_wb_file_path)
    # 2030
    sheet_name = list(generated_df.keys())[0]
    expected_df_2030 = pd.read_excel(expected_wb, sheet_name=sheet_name)
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2030, check_dtype=False)
    # 2035
    sheet_name = list(generated_df.keys())[1]
    expected_df_2035 = pd.read_excel(expected_wb, sheet_name=sheet_name)
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2035, check_dtype=False)
