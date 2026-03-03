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

import re

from pathlib import Path

import pandas as pd

from antares.data_collection.referential_data.main_params import ClusterParams, parse_main_params


def test_parse_main_params_file_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(FileNotFoundError, match=re.escape(f"Input file does not exist: {fake_path}")):
        parse_main_params(file_path=fake_path)


@pytest.mark.parametrize(
    "written_sheets,missing_sheet",
    [
        (["PAYS", "STUDY_SCENARIO", "Common Data"], "CLUSTER"),
        (["CLUSTER", "STUDY_SCENARIO", "Common Data"], "PAYS"),
        (["CLUSTER", "PAYS", "Common Data"], "STUDY_SCENARIO"),
        (["CLUSTER", "PAYS", "STUDY_SCENARIO"], "Common Data"),
    ],
)
def test_parse_main_params_mandatory_sheets(
    tmp_path: Path, written_sheets: tuple[str, str], missing_sheet: str
) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    df = pd.DataFrame({"A": [1, 2, 3]})

    df.to_excel(path_file, sheet_name=written_sheets[0], index=False)
    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        for sheet in written_sheets[1:]:
            df.to_excel(writer, sheet_name=sheet, index=False)

    # then
    with pytest.raises(ValueError, match=f"Worksheet named '{missing_sheet}' not found"):
        parse_main_params(file_path=path_file)


@pytest.mark.parametrize(
    "missing_column",
    [
        {"PAYS": "market_node"},
        {"PAYS": "code_antares"},
        {"STUDY_SCENARIO": "YEAR"},
        {"STUDY_SCENARIO": "STUDY_SCENARIO"},
        {"CLUSTER": "TYPE"},
        {"CLUSTER": "CLUSTER_PEMMDB"},
        {"CLUSTER": "CLUSTER_BP"},
        {"Common Data": "cluster_BP"},
        {"Common Data": "Fuel"},
        {"Common Data": "Type"},
    ],
)
def test_parse_main_params_mandatory_columns(tmp_path: Path, missing_column: dict[str, str]) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    sheets = {
        "PAYS": pd.DataFrame({"market_node": ["ok"], "code_antares": ["ok"]}),
        "STUDY_SCENARIO": pd.DataFrame({"YEAR": [2026], "STUDY_SCENARIO": ["ok"]}),
        "CLUSTER": pd.DataFrame({"TYPE": ["Thermal"], "CLUSTER_PEMMDB": ["ok"], "CLUSTER_BP": ["ok"]}),
        "Common Data": pd.DataFrame({"Type": ["ok"], "Fuel": ["ok"], "cluster_BP": ["ok"]}),
    }
    # Remove the column to create the issue
    data = list(missing_column.items())[0]
    key, value = data[0], data[1]
    sheets[key].drop(value, axis=1, inplace=True)

    sheets["PAYS"].to_excel(path_file, sheet_name="PAYS", index=False)
    del sheets["PAYS"]
    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        for sheet in sheets:
            sheets[sheet].to_excel(writer, sheet_name=sheet, index=False)

    # then
    msg = f"Column '{value}' not found inside sheet '{key}'"
    with pytest.raises(ValueError, match=re.escape(msg)):
        parse_main_params(file_path=path_file)


def test_parse_main_params_real_test_case(tmp_path: Path, mock_parsing_main_params_xlsx: Path) -> None:
    # Use real test case
    file_path = mock_parsing_main_params_xlsx

    main_params = parse_main_params(file_path=file_path)

    # Check `market_to_antares` attribute
    assert main_params._market_to_antares == {"AL00": "AL", "AT00": "AT", "BE00": "BE", "FR00": "FR"}

    assert main_params._year_to_scenario == {2030: "ERAA", 2040: "ERAA", 2060: "TYNDP", 2200: "TYNDP"}

    assert main_params._cluster_pemmdb_to_antares == {
        "Gas/CCGT CCS": "CCGT CCS",
        "OtherNon-RES/Gas/CCGT CCS": "CCGT CCS",
        "Gas/CCGT CCS/CHP": "CCGT CCS CHP",
    }

    assert main_params._cluster_antares == {
        "Nuclear": ClusterParams(type="-", fuel="Nuclear"),
        "Nuclear SMR": ClusterParams(type="SMR", fuel="Nuclear"),
    }
