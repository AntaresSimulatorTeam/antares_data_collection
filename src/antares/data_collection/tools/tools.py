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
from typing import Optional, List

import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.workbook.workbook import Workbook


##
# Data management
##


# TODO add tests
def scenario_filter(
    df_input: pd.DataFrame, filter_params: Optional[List[str]] = None
) -> pd.DataFrame:
    valid_choices: List[str] = ["ERAA", "TYNDP"]

    # default: "ERAA"
    if filter_params is None or len(filter_params) != 1:
        filter_params = ["ERAA"]

    fp: str = filter_params[0]

    if fp not in valid_choices:
        raise ValueError(f"filter_params must be in {valid_choices}")

    filter_map: dict[str, str] = {
        "ERAA": r"ERAA&TYNDP|ERAA",
        "TYNDP": r"ERAA&TYNDP|TYNDP",
    }

    pattern: str = filter_map[fp]

    return df_input[df_input["STUDY_SCENARIO"].str.contains(pattern, regex=True)]


##
# Export : Excel workbook
##


def create_xlsx_workbook(
    path_dir: Path,
    workbook_name: str,
    sheet_name: str,
    data_df: Optional[pd.DataFrame] = None,
    index: bool = False,
    header: bool = True,
    overwrite: bool = False,
) -> None:
    if not path_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {path_dir}")

    if not overwrite:
        name_file = workbook_name + ".xlsx"
        if (path_dir / name_file).exists():
            raise ValueError(f"This Workbook already exist: {name_file}")

    wb = Workbook()
    ws = wb.active
    assert ws is not None  # to prevent error with mypy and "types-openpyxl"

    ws.title = sheet_name

    wb_name = workbook_name + ".xlsx"

    # empty workbook
    if data_df is None:
        wb.save(path_dir / wb_name)

    # workbook with data
    if data_df is not None:
        for r in dataframe_to_rows(data_df, index=index, header=header):
            ws.append(r)

        wb.save(path_dir / wb_name)


# TODO manage editing mode
def edit_xlsx_workbook(
    path_file: Path,
    sheet_name: str,
    data_df: Optional[pd.DataFrame] = None,
    index: bool = False,
    header: bool = True,
    overwrite: bool = False,
) -> None:
    if not path_file.exists():
        raise FileNotFoundError(f"This Excel file does not exist: {path_file}")

    # wb = load_workbook(filename=path_file)
    # wb.get_sheet_names()
    # ws = wb[sheet_name]
