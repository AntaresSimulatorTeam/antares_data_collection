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

import shutil
import time

from pathlib import Path

import pandas as pd

from openpyxl import load_workbook

from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.param_modulation.constants import TECHNICAL_PARAMS_FOLDER
from antares.data_collection.thermal.parsing import ThermalParser
from antares.data_collection.thermal.specific_param.constants import SPECIFIC_PARAM_FOLDER
from antares.data_collection.thermal.specific_param.parsing import ThermalSpecificParamParser
from antares.data_collection.thermal.utils import get_path_capacity_modulation_file
from antares.data_collection.utils import write_csv_file
from tests.conftest import RESOURCE_PATH


@pytest.mark.parametrize("include_hour", [False, True])
def test_capacity_modulation_reader_ignores_time_columns(tmp_path: Path, include_hour: bool) -> None:
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")
    df = pd.DataFrame(
        {
            "DATE_HEURE": ["2028-07-01 00:00:00", "2028-07-01 01:00:00"],
            "AT_CCGT": [0.8, 0.3],
        }
    )
    if include_hour:
        df.insert(1, "HEURE", [4345, 4346])
    write_csv_file(get_path_capacity_modulation_file(2030, tmp_path), df)
    parser = ThermalSpecificParamParser(tmp_path, main_params, [2030])

    assert parser._parse_capacity_ts_modulation_file() == {2030: {"AT": {"CCGT": 0.3}}}


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # copy files capacity modulation to be used to compute min values of time series
    # the min values will be used then to compute `min_stable_gen`
    cm_folder_path = RESOURCE_PATH / "expected_output_files" / "thermal"

    cm_2030_file = cm_folder_path / "CM_PEMMDB_2029-2030.csv"
    cm_2035_file = cm_folder_path / "CM_PEMMDB_2034-2035.csv"

    # create a param modulation folder to copy files in
    cm_test_files_folder = tmp_path / TECHNICAL_PARAMS_FOLDER
    cm_test_files_folder.mkdir(parents=True, exist_ok=True)

    shutil.copy(cm_2030_file, cm_test_files_folder / "CM_PEMMDB_2029-2030.csv")
    shutil.copy(cm_2035_file, cm_test_files_folder / "CM_PEMMDB_2034-2035.csv")

    # Build a thermal specific param file
    op_stat_filter = ["Available on market", "Inelastic supply / fixed profile"]

    parser = ThermalParser(RESOURCE_PATH, tmp_path, op_stat_filter, main_params, [2030, 2035])

    start = time.time()
    parser.build_specific_param()
    end = time.time()
    print("Duration Specific param", end - start)

    # Asserts the file is created
    generated_file_path = tmp_path / SPECIFIC_PARAM_FOLDER / "specific_param_PEMMDB.xlsx"
    assert generated_file_path.exists()

    # read Excel workbook, one sheet by year
    file_wb = pd.ExcelFile(generated_file_path)
    sheet_names = file_wb.sheet_names

    assert sheet_names == ["2029-2030", "2034-2035"]

    # Compare its content with the expected one for any sheet
    expected_file_path = RESOURCE_PATH / "expected_output_files" / "thermal" / "specific_param_PEMMDB.xlsx"
    generated_workbook = load_workbook(generated_file_path)
    try:
        with pd.ExcelFile(expected_file_path) as expected_excel_wb:
            for sheet_name in sheet_names:
                expected_df = pd.read_excel(expected_excel_wb, sheet_name)
                sheet = generated_workbook[str(sheet_name)]
                assert all(cell.value is None for row in sheet.iter_rows(min_row=1, max_row=2) for cell in row)
                assert [cell.value for cell in sheet[3]] == expected_df.columns.tolist()
                assert sheet.cell(row=4, column=1).value == expected_df.iloc[0, 0]

                generated_df = pd.read_excel(generated_file_path, sheet_name=sheet_name, skiprows=2)
                pd.testing.assert_frame_equal(generated_df, expected_df, check_dtype=False)
    finally:
        generated_workbook.close()
