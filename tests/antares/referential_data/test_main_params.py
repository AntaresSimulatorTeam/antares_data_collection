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


def _build_mock_main_params_dict() -> dict[str, pd.DataFrame]:
    return {
        "PAYS": pd.DataFrame({"market_node": ["ok"], "code_antares": ["ok"]}),
        "STUDY_SCENARIO": pd.DataFrame({"YEAR": [2026], "STUDY_SCENARIO": ["ok"]}),
        "CLUSTER": pd.DataFrame(
            {
                "TYPE": ["Thermal"],
                "CLUSTER_PEMMDB": ["ok"],
                "CLUSTER_BP": ["ok"],
                "Technology thermal": ["tech"],
            }
        ),
        "Common Data": pd.DataFrame(
            {
                "cluster_BP": ["ok"],
                "Fuel": ["gas"],
                "efficiency_default": [0.5],
                "FO_rate_default": [0.5],
                "FO_duration_default": [10],
                "PO_duration_default": [10],
                "PO_winter_default": [0.5],
                "min_stable_generation_default": [0.5],
            }
        ),
        "PEAK_PARAMS": pd.DataFrame(
            {
                "hour": [1, 2, 3],
                "period_hour": ["HC", "HC", "HC"],
                "month": [1, 2, 3],
                "period_month": ["winter", "winter", "winter"],
            }
        ),
    }


def test_parse_main_params_file_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(FileNotFoundError, match=re.escape(f"Input file does not exist: {fake_path}")):
        parse_main_params(file_path=fake_path)


@pytest.mark.parametrize(
    "written_sheets,missing_sheet",
    [
        (["PAYS", "STUDY_SCENARIO", "Common Data", "PEAK_PARAMS"], "CLUSTER"),
        (["CLUSTER", "STUDY_SCENARIO", "Common Data", "PEAK_PARAMS"], "PAYS"),
        (["CLUSTER", "PAYS", "Common Data", "PEAK_PARAMS"], "STUDY_SCENARIO"),
        (["CLUSTER", "PAYS", "STUDY_SCENARIO", "PEAK_PARAMS"], "Common Data"),
        (["CLUSTER", "PAYS", "STUDY_SCENARIO", "Common Data"], "PEAK_PARAMS"),
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
        {"CLUSTER": "Technology thermal"},
        {"Common Data": "cluster_BP"},
        {"Common Data": "Fuel"},
        {"Common Data": "efficiency_default"},
        {"Common Data": "FO_rate_default"},
        {"Common Data": "FO_duration_default"},
        {"Common Data": "PO_duration_default"},
        {"Common Data": "PO_winter_default"},
        {"Common Data": "min_stable_generation_default"},
        {"PEAK_PARAMS": "hour"},
        {"PEAK_PARAMS": "period_hour"},
        {"PEAK_PARAMS": "month"},
        {"PEAK_PARAMS": "period_month"},
    ],
)
def test_parse_main_params_mandatory_columns(tmp_path: Path, missing_column: dict[str, str]) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    mocked_main_params = _build_mock_main_params_dict()

    # Remove the column to create the issue
    data = list(missing_column.items())[0]
    key, value = data[0], data[1]
    mocked_main_params[key].drop(value, axis=1, inplace=True)

    mocked_main_params["PAYS"].to_excel(path_file, sheet_name="PAYS", index=False)
    del mocked_main_params["PAYS"]
    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        for sheet in mocked_main_params:
            mocked_main_params[sheet].to_excel(writer, sheet_name=sheet, index=False)

    # then
    msg = f"Column '{value}' not found inside sheet '{key}'"
    with pytest.raises(ValueError, match=re.escape(msg)):
        parse_main_params(file_path=path_file)


@pytest.mark.parametrize(
    "column, invalid_value",
    [
        ("efficiency_default", 1.5),
        ("FO_rate_default", -0.1),
        ("PO_winter_default", 2),
        ("min_stable_generation_default", -1),
    ],
)
def test_common_data_ratio_validation(tmp_path: Path, column: str, invalid_value: int | float) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    mocked_main_params = _build_mock_main_params_dict()

    # inject error
    mocked_main_params["Common Data"].loc[0, column] = invalid_value

    # write excel
    with pd.ExcelWriter(path_file, engine="openpyxl") as writer:
        for name, df in mocked_main_params.items():
            df.to_excel(writer, sheet_name=name, index=False)

    msg = f"Column '{column}' must be between 0 and 1"
    with pytest.raises(ValueError, match=msg):
        parse_main_params(file_path=path_file)


@pytest.mark.parametrize(
    "column, invalid_value",
    [
        ("FO_duration_default", 1.5),
        ("PO_duration_default", -0.1),
    ],
)
def test_common_data_int_validation(tmp_path: Path, column: str, invalid_value: float) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    mocked_main_params = _build_mock_main_params_dict()

    # force cast to float to put invalid value without raise pandas error
    INT_TEST_COLS = ["FO_duration_default", "PO_duration_default"]
    mocked_main_params["Common Data"][INT_TEST_COLS] = mocked_main_params["Common Data"][INT_TEST_COLS].astype(float)

    # inject error
    mocked_main_params["Common Data"].loc[0, column] = invalid_value

    # write excel
    with pd.ExcelWriter(path_file, engine="openpyxl") as writer:
        for name, df in mocked_main_params.items():
            df.to_excel(writer, sheet_name=name, index=False)

    msg = f"Column '{column}' must be integer"
    with pytest.raises(ValueError, match=msg):
        parse_main_params(file_path=path_file)


@pytest.mark.parametrize(
    "wrong_peak_df",
    [
        pd.DataFrame(
            {
                "hour": [1, 2, 3],
                "period_hour": ["HC", "HC", "HC"],
                "month": [1, 2, 3],
                "period_month": ["winter", "winter", "winter"],
            }
        ),
        pd.DataFrame(
            {
                "hour": [1] + list(range(1, 24)),
                "period_hour": ["HC" for i in range(1, 25)],
                "month": list(range(1, 25)),
                "period_month": ["winter" for i in range(1, 25)],
            }
        ),
    ],
)
def test_peak_param_hour_value_validation(tmp_path: Path, wrong_peak_df: pd.DataFrame) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    mocked_main_params = _build_mock_main_params_dict()

    # inject error
    mocked_main_params["PEAK_PARAMS"] = wrong_peak_df

    # write excel
    with pd.ExcelWriter(path_file, engine="openpyxl") as writer:
        for name, df in mocked_main_params.items():
            df.to_excel(writer, sheet_name=name, index=False)

    expected_hours = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
    msg = f"Invalid 'hour' column. Provide: '{expected_hours}'"
    with pytest.raises(ValueError, match=re.escape(msg)):
        parse_main_params(file_path=path_file)


@pytest.mark.parametrize(
    "wrong_peak_df",
    [
        pd.DataFrame(
            {
                "hour": list(range(1, 25)),
                "period_hour": ["HC" for i in range(1, 25)],
                "month": [1] + list(range(1, 12)) + [pd.NA for i in range(1, 13)],
                "period_month": ["winter" for i in range(1, 25)],
            }
        ),
        pd.DataFrame(
            {
                "hour": list(range(1, 25)),
                "period_hour": ["HC" for i in range(1, 25)],
                "month": [1] + list(range(1, 13)) + [pd.NA for i in range(1, 12)],
                "period_month": ["winter" for i in range(1, 25)],
            }
        ),
    ],
)
def test_peak_param_month_value_validation(tmp_path: Path, wrong_peak_df: pd.DataFrame) -> None:
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    mocked_main_params = _build_mock_main_params_dict()

    # inject error
    mocked_main_params["PEAK_PARAMS"] = wrong_peak_df

    # write excel
    with pd.ExcelWriter(path_file, engine="openpyxl") as writer:
        for name, df in mocked_main_params.items():
            df.to_excel(writer, sheet_name=name, index=False)

    expected_months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    msg = f"Invalid 'month' column. Provide: '{expected_months}'"
    with pytest.raises(ValueError, match=re.escape(msg)):
        parse_main_params(file_path=path_file)


def test_main_params_getters() -> None:
    file_path = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"
    main_params = parse_main_params(file_path=file_path)

    # PEAK PARAMS
    assert main_params.get_peak_hour_label(1) == "HC"
    assert main_params.get_peak_hour_label(10) == "HP"
    assert main_params.get_peak_month_label(1) == "winter"
    assert main_params.get_peak_month_label(8) == "summer"


# Default: main_params is available for thermal clusters
def test_parse_main_params_real_test_case_default_thermal(tmp_path: Path) -> None:
    # Use real test case
    file_path = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"

    main_params = parse_main_params(file_path=file_path)

    # Check `market_to_antares` attribute
    assert main_params._market_to_antares == {
        "AL00": np.nan,
        "AT00": "AT",
        "BA00": np.nan,
        "BE00": "BE",
        "BEO1_OFF": "BE",
        "BEO2_OFF": "BE",
        "BG00": np.nan,
        "CH00": "CH",
        "CY00": np.nan,
        "CZ00": "CZ",
        "DE00": "DE",
        "DEKF_OFF": np.nan,
        "DKB2_OFF": np.nan,
        "DKBH_OFF": np.nan,
        "DKE1": "DKe",
        "DKHE_OFF": np.nan,
        "DKK2_OFF": np.nan,
        "DKKA_OFF": np.nan,
        "DKKF_OFF": np.nan,
        "DKN1_OFF": np.nan,
        "DKN2_OFF": np.nan,
        "DKN3_OFF": np.nan,
        "DKN4_OFF": np.nan,
        "DKN5_OFF": np.nan,
        "DKN6_OFF": np.nan,
        "DKN7_OFF": np.nan,
        "DKN8_OFF": np.nan,
        "DKN9_OFF": np.nan,
        "DKNS_OFF": np.nan,
        "DKW1": "DKw",
        "EE00": np.nan,
        "ES00": "ES",
        "FI00": np.nan,
        "FR00": np.nan,
        "FR15": np.nan,
        "GR00": np.nan,
        "GR03": np.nan,
        "HR00": np.nan,
        "HU00": np.nan,
        "IE00": "IE",
        "ITCA": "ITca",
        "ITCN": "ITcn",
        "ITCS": "ITcs",
        "ITN1": "ITn",
        "ITS1": "ITs",
        "ITSA": "ITsar",
        "ITSI": "ITsic",
        "LT00": np.nan,
        "LTH1_OFF": np.nan,
        "LUB1": "LU",
        "LUF1": "LU",
        "LUG1": "LU",
        "LUV1": "LU",
        "LV00": np.nan,
        "MD00": np.nan,
        "ME00": np.nan,
        "MK00": np.nan,
        "MT00": np.nan,
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
        "NLLL_OFF": "NL",
        "NOM1": "NOm",
        "NON1": "NOn",
        "NONC_OFF": np.nan,
        "NOND_OFF": np.nan,
        "NOS1": "NOs",
        "NOS2": "NOs",
        "NOS3": "NOs",
        "NOSF_OFF": "NOs",
        "NOWB_OFF": np.nan,
        "NOWF_OFF": np.nan,
        "PL00": "PL",
        "PLE0": "PL",
        "PLI0": "PL",
        "PT00": "PT",
        "RO00": np.nan,
        "RS00": np.nan,
        "SE01": "SE1",
        "SE02": "SE2",
        "SE03": "SE3",
        "SE04": "SE4",
        "SI00": np.nan,
        "SK00": np.nan,
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
        "OtherNon-RES/Lignite/old 1": "Lignite old 1",
        "OtherNon-RES/Hydrogen/CCGT": "CCGT H2",
        "OtherNon-RES/Light oil/-": "Light oil",
        "OtherNon-RES/Hydrogen/OCGT": "OCGT H2",
    }

    assert main_params._cluster_antares == {
        "Nuclear": ClusterParams(
            technology="Nuclear",
            fuel="Nuclear",
            efficiency_default=0.33,
            fo_rate_default=0.05,
            fo_duration_default=7,
            po_duration_default=54,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Nuclear SMR": ClusterParams(
            technology="Nuclear",
            fuel="Nuclear",
            efficiency_default=0.33,
            fo_rate_default=0.05,
            fo_duration_default=7,
            po_duration_default=54,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Hard coal old 1": ClusterParams(
            technology="Coal and lignite",
            fuel="Hard coal",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Hard coal old 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Hard coal",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Hard coal old 2": ClusterParams(
            technology="Coal and lignite",
            fuel="Hard coal",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Hard coal old 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Hard coal",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Hard coal new": ClusterParams(
            technology="Coal and lignite",
            fuel="Hard coal",
            efficiency_default=0.46,
            fo_rate_default=0.075,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.25,
        ),
        "Hard coal new CHP": ClusterParams(
            technology="CHP",
            fuel="Hard coal",
            efficiency_default=0.46,
            fo_rate_default=0.075,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.25,
        ),
        "Lignite old 1": ClusterParams(
            technology="Coal and lignite",
            fuel="Lignite",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Lignite old 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Lignite",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Lignite old 2": ClusterParams(
            technology="Coal and lignite",
            fuel="Lignite",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Lignite old 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Lignite",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Lignite new": ClusterParams(
            technology="Coal and lignite",
            fuel="Lignite",
            efficiency_default=0.46,
            fo_rate_default=0.075,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Lignite new CHP": ClusterParams(
            technology="CHP",
            fuel="Lignite",
            efficiency_default=0.46,
            fo_rate_default=0.075,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Gas conventional old 1": ClusterParams(
            technology="Gas conventional",
            fuel="Gas",
            efficiency_default=0.36,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.2,
        ),
        "Gas conventional old 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.36,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.2,
        ),
        "Gas conventional old 2": ClusterParams(
            technology="Gas conventional",
            fuel="Gas",
            efficiency_default=0.41,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.2,
        ),
        "Gas conventional old 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.41,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.2,
        ),
        "CCGT old 1": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.4,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "CCGT old 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.4,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "CCGT old 2": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.48,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "CCGT old 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.48,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "CCGT present 1": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.56,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT present 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.56,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT present 2": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.58,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT present 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.58,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT new": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.6,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT new CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.6,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT CCS": ClusterParams(
            technology="CCGT",
            fuel="Gas",
            efficiency_default=0.51,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT CCS CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.51,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "OCGT old": ClusterParams(
            technology="OCGT",
            fuel="Gas",
            efficiency_default=0.35,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "OCGT old CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.35,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "OCGT new": ClusterParams(
            technology="OCGT",
            fuel="Gas",
            efficiency_default=0.42,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "OCGT new CHP": ClusterParams(
            technology="CHP",
            fuel="Gas",
            efficiency_default=0.42,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "Light oil": ClusterParams(
            technology="OCGT",
            fuel="Oil",
            efficiency_default=0.35,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Light oil CHP": ClusterParams(
            technology="CHP",
            fuel="Oil",
            efficiency_default=0.35,
            fo_rate_default=0.08,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Heavy oil old 1": ClusterParams(
            technology="Heavy oil",
            fuel="Oil",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Heavy oil old 1 CHP": ClusterParams(
            technology="CHP",
            fuel="Oil",
            efficiency_default=0.35,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Heavy oil old 2": ClusterParams(
            technology="Heavy oil",
            fuel="Oil",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Heavy oil old 2 CHP": ClusterParams(
            technology="CHP",
            fuel="Oil",
            efficiency_default=0.4,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Oil shale old": ClusterParams(
            technology="?",
            fuel="Oil",
            efficiency_default=0.29,
            fo_rate_default=0.1,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "Oil shale new": ClusterParams(
            technology="?",
            fuel="Oil",
            efficiency_default=0.39,
            fo_rate_default=0.075,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.5,
        ),
        "CCGT H2": ClusterParams(
            technology="CCGT",
            fuel="H2",
            efficiency_default=0.6,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "CCGT H2 CHP": ClusterParams(
            technology="CHP",
            fuel="H2",
            efficiency_default=0.6,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=27,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "OCGT H2": ClusterParams(
            technology="OCGT",
            fuel="H2",
            efficiency_default=0.42,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
        "OCGT H2 CHP": ClusterParams(
            technology="CHP",
            fuel="H2",
            efficiency_default=0.42,
            fo_rate_default=0.05,
            fo_duration_default=1,
            po_duration_default=13,
            po_winter_default=0.15,
            min_stable_generation_default=0.4,
        ),
    }

    assert main_params._peak_hour_label == {
        1: "HC",
        2: "HC",
        3: "HC",
        4: "HC",
        5: "HC",
        6: "HC",
        7: "HC",
        8: "HC",
        9: "HP",
        10: "HP",
        11: "HP",
        12: "HP",
        13: "HP",
        14: "HP",
        15: "HP",
        16: "HP",
        17: "HP",
        18: "HP",
        19: "HP",
        20: "HP",
        21: "HC",
        22: "HC",
        23: "HC",
        24: "HC",
    }

    assert main_params._peak_month_label == {
        1.0: "winter",
        2.0: "winter",
        3.0: "winter",
        4.0: "summer",
        5.0: "summer",
        6.0: "summer",
        7.0: "summer",
        8.0: "summer",
        9.0: "summer",
        10.0: "winter",
        11.0: "winter",
        12.0: "winter",
    }


def test_parse_main_params_real_test_case_misc(tmp_path: Path) -> None:
    # Use real test case
    file_path = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"

    main_params = parse_main_params(file_path=file_path, cluster_type="misc")

    # NO NEED to parse `Common Data` sheet
    assert main_params._cluster_antares == {}

    # "MISC"
    assert main_params._cluster_pemmdb_to_antares == {
        "Marine": "wave",
        "Waste": "waste",
        "Small biomass": "biomass",
        "Geothermal": "geothermal",
        "Not defined or splitting not known RES": "other",
    }

    # Check `market_to_antares` attribute
    assert main_params._market_to_antares == {
        "AL00": np.nan,
        "AT00": "AT",
        "BA00": np.nan,
        "BE00": "BE",
        "BEO1_OFF": "BE",
        "BEO2_OFF": "BE",
        "BG00": np.nan,
        "CH00": "CH",
        "CY00": np.nan,
        "CZ00": "CZ",
        "DE00": "DE",
        "DEKF_OFF": np.nan,
        "DKB2_OFF": np.nan,
        "DKBH_OFF": np.nan,
        "DKE1": "DKe",
        "DKHE_OFF": np.nan,
        "DKK2_OFF": np.nan,
        "DKKA_OFF": np.nan,
        "DKKF_OFF": np.nan,
        "DKN1_OFF": np.nan,
        "DKN2_OFF": np.nan,
        "DKN3_OFF": np.nan,
        "DKN4_OFF": np.nan,
        "DKN5_OFF": np.nan,
        "DKN6_OFF": np.nan,
        "DKN7_OFF": np.nan,
        "DKN8_OFF": np.nan,
        "DKN9_OFF": np.nan,
        "DKNS_OFF": np.nan,
        "DKW1": "DKw",
        "EE00": np.nan,
        "ES00": "ES",
        "FI00": np.nan,
        "FR00": np.nan,
        "FR15": np.nan,
        "GR00": np.nan,
        "GR03": np.nan,
        "HR00": np.nan,
        "HU00": np.nan,
        "IE00": "IE",
        "ITCA": "ITca",
        "ITCN": "ITcn",
        "ITCS": "ITcs",
        "ITN1": "ITn",
        "ITS1": "ITs",
        "ITSA": "ITsar",
        "ITSI": "ITsic",
        "LT00": np.nan,
        "LTH1_OFF": np.nan,
        "LUB1": "LU",
        "LUF1": "LU",
        "LUG1": "LU",
        "LUV1": "LU",
        "LV00": np.nan,
        "MD00": np.nan,
        "ME00": np.nan,
        "MK00": np.nan,
        "MT00": np.nan,
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
        "NLLL_OFF": "NL",
        "NOM1": "NOm",
        "NON1": "NOn",
        "NONC_OFF": np.nan,
        "NOND_OFF": np.nan,
        "NOS1": "NOs",
        "NOS2": "NOs",
        "NOS3": "NOs",
        "NOSF_OFF": "NOs",
        "NOWB_OFF": np.nan,
        "NOWF_OFF": np.nan,
        "PL00": "PL",
        "PLE0": "PL",
        "PLI0": "PL",
        "PT00": "PT",
        "RO00": np.nan,
        "RS00": np.nan,
        "SE01": "SE1",
        "SE02": "SE2",
        "SE03": "SE3",
        "SE04": "SE4",
        "SI00": np.nan,
        "SK00": np.nan,
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

    assert main_params._peak_hour_label == {
        1: "HC",
        2: "HC",
        3: "HC",
        4: "HC",
        5: "HC",
        6: "HC",
        7: "HC",
        8: "HC",
        9: "HP",
        10: "HP",
        11: "HP",
        12: "HP",
        13: "HP",
        14: "HP",
        15: "HP",
        16: "HP",
        17: "HP",
        18: "HP",
        19: "HP",
        20: "HP",
        21: "HC",
        22: "HC",
        23: "HC",
        24: "HC",
    }

    assert main_params._peak_month_label == {
        1.0: "winter",
        2.0: "winter",
        3.0: "winter",
        4.0: "summer",
        5.0: "summer",
        6.0: "summer",
        7.0: "summer",
        8.0: "summer",
        9.0: "summer",
        10.0: "winter",
        11.0: "winter",
        12.0: "winter",
    }
