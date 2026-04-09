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
import shutil
import time

from pathlib import Path

import pandas as pd

from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.param_modulation.constants import TECHNICAL_PARAMS_FOLDER
from antares.data_collection.thermal.parsing import ThermalParser
from antares.data_collection.thermal.specific_param.constants import SPECIFIC_PARAM_FOLDER
from tests.conftest import RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # copy files capacity modulation to be used to compute min values of time series
    # the min values will be used then to compute `min_stable_gen`
    cm_folder_path = RESOURCE_PATH / "expected_output_files" / "thermal"

    cm_2030_file = cm_folder_path / "CM_PEMMDB_2029-2030.csv"
    cm_2035_file = cm_folder_path / "CM_PEMMDB_2034-2035.csv"

    # create param modulation folder to copy files in
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

    generated_df = pd.read_excel(generated_file_path, sheet_name=sheet_names)
    assert list(generated_df.keys()) == ["2029-2030", "2034-2035"]

    # Compare its content with the expected one for any sheet
    # 2030
    expected_file_path = RESOURCE_PATH / "expected_output_files" / "thermal" / "specific_2030.xlsx"
    expected_df_2030 = pd.read_excel(expected_file_path)
    sheet_name = list(generated_df.keys())[0]
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2030, check_dtype=False)
    # 2035
    expected_file_path = RESOURCE_PATH / "expected_output_files" / "thermal" / "specific_2035.xlsx"
    expected_df_2035 = pd.read_excel(expected_file_path)
    sheet_name = list(generated_df.keys())[1]
    pd.testing.assert_frame_equal(generated_df[sheet_name], expected_df_2035, check_dtype=False)
