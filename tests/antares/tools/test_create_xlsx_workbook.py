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

import os
import re
import pytest
from pathlib import Path

import pandas as pd
from openpyxl.reader.excel import load_workbook

from antares.data_collection.tools.tools import create_xlsx_workbook, edit_xlsx_workbook
# from tests.antares.links.test_links import mock_links_main_params_xlsx


##
# Creation
##


def test_create_workbook_dir_not_exist(tmp_path: Path) -> None:
    dir_export_path = tmp_path / "toto"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"Input directory does not exist: {dir_export_path}"),
    ):
        create_xlsx_workbook(
            path_dir=dir_export_path, workbook_name="dir_not_exist", sheet_name="sheet"
        )


def test_create_empty_workbook(tmp_path: Path) -> None:
    dir_export_path = tmp_path / "links_data_export"
    os.makedirs(dir_export_path)

    # when
    create_xlsx_workbook(
        path_dir=dir_export_path,
        workbook_name="empty_workbook",
        sheet_name="empty_sheet",
    )
    # then
    assert os.path.exists(dir_export_path / "empty_workbook.xlsx")

    wb = load_workbook(filename=dir_export_path / "empty_workbook.xlsx")
    assert wb.sheetnames == ["empty_sheet"]


def test_create_workbook_with_data(tmp_path: Path) -> None:
    dir_export_path = tmp_path / "links_data_export"
    os.makedirs(dir_export_path)

    # given
    df_pandaset = pd.DataFrame(
        {
            "YEAR": ["2030", "2040", "2060"],
            "STUDY_SCENARIO": ["ERAA", "ERAA", "TYNDP"],
        }
    )

    # when
    create_xlsx_workbook(
        path_dir=dir_export_path,
        workbook_name="test_workbook",
        sheet_name="data_sheet",
        data_df=df_pandaset,
    )

    # then
    dtype = {"YEAR": object, "STUDY_SCENARIO": object}
    df_to_test = pd.read_excel(dir_export_path / "test_workbook.xlsx", dtype=dtype)

    assert list(df_to_test.columns) == list(df_pandaset.columns)
    assert df_to_test.shape == df_pandaset.shape
    assert df_to_test.equals(df_pandaset)


##
# Edition
##


def test_edit_workbook_file_not_exist(tmp_path: Path) -> None:
    dir_export_path = tmp_path / "toto.xlsx"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"This Excel file does not exist: {dir_export_path}"),
    ):
        edit_xlsx_workbook(path_file=dir_export_path, sheet_name="sheet")


def test_edit_workbook_file_exist(mock_links_main_params_xlsx: Path) -> None:
    # when
    edit_xlsx_workbook(
        path_file=mock_links_main_params_xlsx, sheet_name="STUDY_SCENARIO"
    )
