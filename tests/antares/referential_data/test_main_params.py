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

from antares.data_collection.referential_data.main_params import MainParams, parse_main_params


def test_parse_main_params_file_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(FileNotFoundError, match=re.escape(f"Input file does not exist: {fake_path}")):
        parse_main_params(file_path=fake_path)


@pytest.mark.parametrize(
    "written_sheets,missing_sheet",
    [
        (["PAYS", "STUDY_SCENARIO"], "CLUSTER"),
        (["CLUSTER", "STUDY_SCENARIO"], "PAYS"),
        (["CLUSTER", "PAYS"], "STUDY_SCENARIO"),
    ],
)
def test_parse_main_params_mandatory_sheets(
    tmp_path: Path, written_sheets: tuple[str, str], missing_sheet: str
) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    df = pd.DataFrame({"A": [1, 2, 3]})

    df.to_excel(path_file, sheet_name=written_sheets[0], index=False)
    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        df.to_excel(writer, sheet_name=written_sheets[1], index=False)

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
    ],
)
def test_parse_main_params_mandatory_columns(tmp_path: Path, missing_column: dict[str, str]) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    sheets = {
        "PAYS": pd.DataFrame({"market_node": ["ok"], "code_antares": ["ok"]}),
        "STUDY_SCENARIO": pd.DataFrame({"YEAR": [2026], "STUDY_SCENARIO": ["ok"]}),
        "CLUSTER": pd.DataFrame({"TYPE": ["Thermal"], "CLUSTER_PEMMDB": ["ok"], "CLUSTER_BP": ["ok"]}),
    }
    # Remove the column to create the issue
    data = list(missing_column.items())[0]
    key, value = data[0], data[1]
    sheets[key].drop(value, axis=1, inplace=True)

    sheets["PAYS"].to_excel(path_file, sheet_name="PAYS", index=False)
    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        sheets["STUDY_SCENARIO"].to_excel(writer, sheet_name="STUDY_SCENARIO", index=False)
        sheets["CLUSTER"].to_excel(writer, sheet_name="CLUSTER", index=False)

    # then
    msg = f"Column '{value}' not found inside sheet '{key}'"
    with pytest.raises(ValueError, match=re.escape(msg)):
        parse_main_params(file_path=path_file)


def test_parse_main_params_works(tmp_path: Path) -> None:
    # given
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    ref_pays = pd.DataFrame(
        {
            "areas": ["ok", "ok"],
            "Nom_pays": ["ok", "ok"],
            "code_pays": ["ok", "ok"],
            "market_node": ["a", "b"],
            "code_antares": ["A", "B"],
        }
    )

    ref_scenario = pd.DataFrame({"YEAR": [2020, 2021], "STUDY_SCENARIO": ["ERAA", "TYNDP"]})

    ref_cluster = pd.DataFrame({"TYPE": ["Thermal", "RES"], "CLUSTER_PEMMDB": ["a", "b"], "CLUSTER_BP": ["A", "B"]})

    # write test workbook
    ref_pays.to_excel(path_file, sheet_name="PAYS", index=False)

    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        ref_scenario.to_excel(writer, sheet_name="STUDY_SCENARIO", index=False)
        ref_cluster.to_excel(writer, sheet_name="CLUSTER", index=False)

    # when
    main_params = parse_main_params(file_path=path_file)

    # then
    assert isinstance(main_params, MainParams)

    assert isinstance(main_params._market_to_antares, dict)
    assert isinstance(main_params._year_to_scenario, dict)
    assert isinstance(main_params._cluster_pemmdb_to_antares, dict)

    assert main_params.get_antares_code("a") == "A"
    assert main_params.get_antares_codes(["a", "b"]) == ["A", "B"]
    with pytest.raises(ValueError, match=re.escape("No antares code defined for market c")):
        main_params.get_antares_code("c")

    assert main_params.get_scenario_type(2020) == "ERAA"
    assert main_params.get_scenario_types([2020, 2021]) == ["ERAA", "TYNDP"]
    with pytest.raises(
        ValueError,
        match=re.escape("No scenario defined for year 2019"),
    ):
        main_params.get_scenario_type(2019)

    assert main_params.get_cluster_bp("a") == "A"
    assert main_params.get_clusters_bp(["a", "b"]) == ["A", "B"]
    with pytest.raises(
        ValueError,
        match=re.escape("No cluster BP defined for cluster c"),
    ):
        main_params.get_cluster_bp("c")
