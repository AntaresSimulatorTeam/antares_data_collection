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

import numpy as np
import pandas as pd

from antares.data_collection.referential_data.main_params import ClusterParams, parse_main_params
from tests.conftest import RESOURCE_PATH


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


def test_parse_main_params_real_test_case(tmp_path: Path) -> None:
    # Use real test case
    file_path = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"

    main_params = parse_main_params(file_path=file_path)

    # Check `market_to_antares` attribute
    assert main_params._market_to_antares == {
        "AL00": "AL",
        "AT00": "AT",
        "BA00": "BA",
        "BE00": "BE",
        "BEO1_OFF": "BEo1",
        "BEO2_OFF": "BE",
        "BG00": "BG",
        "CH00": "CH",
        "CY00": "CY",
        "CZ00": "CZ",
        "DE00": "DE",
        "DEKF_OFF": "DEkf",
        "DKB2_OFF": "DKbh",
        "DKBH_OFF": "DKbh",
        "DKE1": "DKe",
        "DKHE_OFF": "DKe",
        "DKK2_OFF": "DKe",
        "DKKA_OFF": "DKw",
        "DKKF_OFF": "DKkf",
        "DKN1_OFF": "DKw",
        "DKN2_OFF": "DKw",
        "DKN3_OFF": "DKw",
        "DKN4_OFF": "DKw",
        "DKN5_OFF": "DKw",
        "DKN6_OFF": "DKw",
        "DKN7_OFF": "DKw",
        "DKN8_OFF": "DKw",
        "DKN9_OFF": "DKkf",
        "DKNS_OFF": "DKns",
        "DKW1": "DKw",
        "EE00": "EE",
        "ES00": "ES",
        "FI00": "FI",
        "FR00": "FR",
        "FR15": np.nan,
        "GR00": "GR",
        "GR03": "GR",
        "HR00": "HR",
        "HU00": "HU",
        "IE00": "IE",
        "ITCA": "ITca",
        "ITCN": "ITcn",
        "ITCS": "ITcs",
        "ITN1": "ITn",
        "ITS1": "ITs",
        "ITSA": "ITsar",
        "ITSI": "ITsic",
        "LT00": "LT",
        "LTH1_OFF": "LT",
        "LUB1": np.nan,
        "LUF1": np.nan,
        "LUG1": "LU",
        "LUV1": "LU",
        "LV00": "LV",
        "MD00": np.nan,
        "ME00": "ME",
        "MK00": "MK",
        "MT00": "MT",
        "NL00": "NL",
        "NL0A_OFF": "NL",
        "NL0B_OFF": "NL",
        "NL0C_OFF": "NL",
        "NL0D_OFF": "NL",
        "NL0E_OFF": "NL",
        "NL0F_OFF": "NL",
        "NL0G_OFF": "NL",
        "NL0J_OFF": "NL",
        "NL0K_OFF": "NL",
        "NL0L_OFF": "NL",
        "NL0M_OFF": "NL",
        "NL0N_OFF": "NL",
        "NL0P_OFF": "NL",
        "NL0Q_OFF": "NL",
        "NL0R_OFF": "NL",
        "NL0S_OFF": "NL",
        "NL0T_OFF": "NL",
        "NL0U_OFF": "NL",
        "NL0V_OFF": "NL",
        "NL0W_OFF": "NL",
        "NL0X_OFF": "NL",
        "NL0Y_OFF": "NL",
        "NLLL_OFF": "NLll",
        "NOM1": "NOm",
        "NON1": "NOn",
        "NONC_OFF": "NOm",
        "NOND_OFF": "NOn",
        "NOS1": "NOs",
        "NOS2": "NOs",
        "NOS3": "NOs",
        "NOSF_OFF": "NOs",
        "NOWB_OFF": "NOs",
        "NOWF_OFF": "NOs",
        "PL00": "PL",
        "PT00": "PT",
        "RO00": "RO",
        "RS00": "RS",
        "SE01": "SE1",
        "SE02": "SE2",
        "SE03": "SE3",
        "SE04": "SE4",
        "SI00": "SI",
        "SK00": "SK",
        "TR00": np.nan,
        "UA00": np.nan,
        "UK00": "UKgb",
        "UKNI": "UKni",
    }

    assert main_params._year_to_scenario == {
        2020: "ERAA",
        2021: "ERAA",
        2022: "ERAA",
        2023: "ERAA",
        2024: "ERAA",
        2025: "ERAA",
        2026: "ERAA",
        2027: "ERAA",
        2028: "ERAA",
        2029: "ERAA",
        2030: "ERAA",
        2031: "ERAA",
        2032: "ERAA",
        2033: "ERAA",
        2034: "ERAA",
        2035: "ERAA",
        2036: "TYNDP",
        2037: "TYNDP",
        2038: "TYNDP",
        2039: "TYNDP",
        2040: "TYNDP",
        2041: "TYNDP",
        2042: "TYNDP",
        2043: "TYNDP",
        2044: "TYNDP",
        2045: "TYNDP",
        2046: "TYNDP",
        2047: "TYNDP",
        2048: "TYNDP",
        2049: "TYNDP",
        2050: "TYNDP",
        2051: "TYNDP",
        2052: "TYNDP",
        2053: "TYNDP",
        2054: "TYNDP",
        2055: "TYNDP",
        2056: "TYNDP",
        2057: "TYNDP",
        2058: "TYNDP",
        2059: "TYNDP",
        2060: "TYNDP",
    }

    assert main_params._cluster_pemmdb_to_antares == {
        "Gas/CCGT CCS": "CCGT CCS",
        "OtherNon-RES/Gas/CCGT CCS": "CCGT CCS",
        "Gas/CCGT CCS/CHP": "CCGT CCS CHP",
        "OtherNon-RES/Gas/CCGT CCS/CHP": "CCGT CCS CHP",
        "Hydrogen/CCGT": "CCGT H2",
        "Hydrogen/CCGT/CHP": "CCGT H2 CHP",
        "OtherNon-RES/Hydrogen/CCGT/CHP": "CCGT H2 CHP",
        "Gas/CCGT new": "CCGT new",
        "OtherNon-RES/Gas/CCGT new": "CCGT new",
        "Gas/CCGT new/CHP": "CCGT new CHP",
        "Gas/CCGT old 1": "CCGT old 1",
        "OtherNon-RES/Gas/CCGT old 1": "CCGT old 1",
        "Gas/CCGT old 1/CHP": "CCGT old 1 CHP",
        "OtherNon-RES/Gas/CCGT old 1/CHP": "CCGT old 1 CHP",
        "Gas/CCGT old 2": "CCGT old 2",
        "OtherNon-RES/Gas/CCGT old 2": "CCGT old 2",
        "Gas/CCGT old 2/CHP": "CCGT old 2 CHP",
        "OtherNon-RES/Gas/CCGT old 2/CHP": "CCGT old 2 CHP",
        "Gas/CCGT present 1": "CCGT present 1",
        "Gas/CCGT present 1/CHP": "CCGT present 1 CHP",
        "Gas/CCGT present 2": "CCGT present 2",
        "OtherNon-RES/Gas/CCGT present 2": "CCGT present 2",
        "Gas/CCGT present 2/CHP": "CCGT present 2 CHP",
        "Fuel cell": "Fuel cell",
        "Gas/conventional old 1": "Gas conventional old 1",
        "OtherNon-RES/Gas/conventional old 1": "Gas conventional old 1",
        "Gas/conventional old 1/CHP": "Gas conventional old 1 CHP",
        "OtherNon-RES/Gas/conventional old 1/CHP": "Gas conventional old 1 CHP",
        "Gas/conventional old 2": "Gas conventional old 2",
        "OtherNon-RES/Gas/conventional old 2": "Gas conventional old 2",
        "Gas/conventional old 2/CHP": "Gas conventional old 2 CHP",
        "OtherNon-RES/Gas/conventional old 2/CHP": "Gas conventional old 2 CHP",
        "Hard coal/new": "Hard coal new",
        "Hard coal/new/CHP": "Hard coal new CHP",
        "Hard coal/old 1": "Hard coal old 1",
        "OtherNon-RES/Hard coal/old 1": "Hard coal old 1",
        "Hard coal/old 1/CHP": "Hard coal old 1 CHP",
        "Hard coal/old 2": "Hard coal old 2",
        "Hard coal/old 2/CHP": "Hard coal old 2 CHP",
        "Heavy oil/old 1": "Heavy oil old 1",
        "Heavy oil/old 1/CHP": "Heavy oil old 1 CHP",
        "Heavy oil/old 2": "Heavy oil old 2",
        "Heavy oil/old 2/CHP": "Heavy oil old 2 CHP",
        "Light oil/-": "Light oil",
        "Light oil/-/CHP": "Light oil CHP",
        "OtherNon-RES/Light oil/-/CHP": "Light oil CHP",
        "Lignite/new": "Lignite new",
        "OtherNon-RES/Lignite/new": "Lignite new",
        "Lignite/new/CHP": "Lignite new CHP",
        "Lignite/old 1": "Lignite old 1",
        "Lignite/old 1/CHP": "Lignite old 1 CHP",
        "OtherNon-RES/Lignite/old 1/CHP": "Lignite old 1 CHP",
        "Lignite/old 2": "Lignite old 2",
        "Lignite/old 2/CHP": "Lignite old 2 CHP",
        "Nuclear/-": "Nuclear",
        "Nuclear/SMR": "Nuclear SMR",
        "Hydrogen/OCGT": "OCGT H2",
        "Hydrogen/OCGT/CHP": "OCGT H2 CHP",
        "OtherNon-RES/Hydrogen/OCGT/CHP": "OCGT H2 CHP",
        "Gas/OCGT new": "OCGT new",
        "OtherNon-RES/Gas/OCGT new": "OCGT new",
        "Gas/OCGT new/CHP": "OCGT new CHP",
        "Gas/OCGT old": "OCGT old",
        "OtherNon-RES/Gas/OCGT old": "OCGT old",
        "Gas/OCGT old/CHP": "OCGT old CHP",
        "OtherNon-RES/Gas/OCGT old/CHP": "OCGT old CHP",
        "Shale oil/new": "Oil shale new",
        "OtherNon-RES/Shale oil/old": "Oil shale old",
        "Shale oil/old": "Oil shale old",
    }

    assert main_params._cluster_antares == {
        "CCGT CCS": ClusterParams(type="CCGT CCS", fuel="Gas"),
        "CCGT CCS CHP": ClusterParams(type="CCGT CCS", fuel="Gas"),
        "CCGT H2": ClusterParams(type="CCGT new", fuel="H2"),
        "CCGT H2 CHP": ClusterParams(type="CCGT new", fuel="H2"),
        "CCGT new": ClusterParams(type="CCGT new", fuel="Gas"),
        "CCGT new CHP": ClusterParams(type="CCGT new", fuel="Gas"),
        "CCGT old 1": ClusterParams(type="CCGT old 1", fuel="Gas"),
        "CCGT old 1 CHP": ClusterParams(type="CCGT old 1", fuel="Gas"),
        "CCGT old 2": ClusterParams(type="CCGT old 2", fuel="Gas"),
        "CCGT old 2 CHP": ClusterParams(type="CCGT old 2", fuel="Gas"),
        "CCGT present 1": ClusterParams(type="CCGT present 1", fuel="Gas"),
        "CCGT present 1 CHP": ClusterParams(type="CCGT present 1", fuel="Gas"),
        "CCGT present 2": ClusterParams(type="CCGT present 2", fuel="Gas"),
        "CCGT present 2 CHP": ClusterParams(type="CCGT present 2", fuel="Gas"),
        "Gas conventional old 1": ClusterParams(type="conventional old 1", fuel="Gas"),
        "Gas conventional old 1 CHP": ClusterParams(type="conventional old 1", fuel="Gas"),
        "Gas conventional old 2": ClusterParams(type="conventional old 2", fuel="Gas"),
        "Gas conventional old 2 CHP": ClusterParams(type="conventional old 2", fuel="Gas"),
        "Hard coal new": ClusterParams(type="new", fuel="Hard coal"),
        "Hard coal new CHP": ClusterParams(type="new", fuel="Hard coal"),
        "Hard coal old 1": ClusterParams(type="old 1", fuel="Hard coal"),
        "Hard coal old 1 CHP": ClusterParams(type="old 1", fuel="Hard coal"),
        "Hard coal old 2": ClusterParams(type="old 2", fuel="Hard coal"),
        "Hard coal old 2 CHP": ClusterParams(type="old 2", fuel="Hard coal"),
        "Heavy oil old 1": ClusterParams(type="old 1", fuel="Oil"),
        "Heavy oil old 1 CHP": ClusterParams(type="old 1", fuel="Oil"),
        "Heavy oil old 2": ClusterParams(type="old 2", fuel="Oil"),
        "Heavy oil old 2 CHP": ClusterParams(type="old 2", fuel="Oil"),
        "Light oil": ClusterParams(type="-", fuel="Oil"),
        "Light oil CHP": ClusterParams(type="-", fuel="Oil"),
        "Lignite new": ClusterParams(type="new", fuel="Lignite"),
        "Lignite new CHP": ClusterParams(type="new", fuel="Lignite"),
        "Lignite old 1": ClusterParams(type="old 1", fuel="Lignite"),
        "Lignite old 1 CHP": ClusterParams(type="old 1", fuel="Lignite"),
        "Lignite old 2": ClusterParams(type="old 2", fuel="Lignite"),
        "Lignite old 2 CHP": ClusterParams(type="old 2", fuel="Lignite"),
        "Nuclear": ClusterParams(type="-", fuel="Nuclear"),
        "Nuclear SMR": ClusterParams(type="SMR", fuel="Nuclear"),
        "OCGT H2": ClusterParams(type="OCGT new", fuel="H2"),
        "OCGT H2 CHP": ClusterParams(type="OCGT new", fuel="H2"),
        "OCGT new": ClusterParams(type="OCGT new", fuel="Gas"),
        "OCGT new CHP": ClusterParams(type="OCGT new", fuel="Gas"),
        "OCGT old": ClusterParams(type="OCGT old", fuel="Gas"),
        "OCGT old CHP": ClusterParams(type="OCGT old", fuel="Gas"),
        "Oil shale new": ClusterParams(type="new", fuel="Oil"),
        "Oil shale old": ClusterParams(type="old", fuel="Oil"),
    }
