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

from antares.data_collection.links import links, conf_links
from antares.data_collection.links.conf_links import ExportLinksColumnsNames
from antares.data_collection.links.links import links_manage_export
from antares.data_collection.tools.conf import LocalConfiguration

# global
ROOT_TEST = Path(__file__).resolve().parents[2]
LINKS_DATA_DIR = ROOT_TEST / "antares" / "links" / "data_test"


# read parquet data test to write .csv
@pytest.fixture
def parquet_to_csv(tmp_path: Path) -> Path:
    # read parquet files
    df_tf = pd.read_parquet(LINKS_DATA_DIR / "transfer_links.parquet")
    df_index = pd.read_parquet(LINKS_DATA_DIR / "ntc_index.parquet")
    df_ntc = pd.read_parquet(LINKS_DATA_DIR / "ntc.parquet")

    output_path = tmp_path / "links_data"
    os.makedirs(output_path)

    # write .csv files
    df_tf.to_csv(output_path / "Transfer Links.csv", index=False)
    df_index.to_csv(output_path / "NTCs Index.csv", index=False)
    df_ntc.to_csv(output_path / "NTCs.csv", index=False)

    return output_path


## mock referential MAIN_PARAMS
@pytest.fixture
def mock_links_main_params_xlsx(tmp_path: Path) -> Path:
    data = {
        "PAYS": pd.DataFrame(
            {
                "Nom_pays": ["Albanie", "Autriche", "Belgique", "France"],
                "code_pays": ["AL", "AT", "BE", "FR"],
                "areas": ["Albanie", "Autriche", "Belgique", "France"],
                "market_node": ["AL00", "AT00", "BE00", "FR00"],
                "code_antares": ["AL", "AT", "BE", "FR"],
            }
        ),
        "STUDY_SCENARIO": pd.DataFrame(
            {
                "YEAR": ["2030", "2040", "2060"],
                "STUDY_SCENARIO": ["ERAA", "ERAA", "TYNDP"],
            }
        ),
        "LINKS": pd.DataFrame(
            {
                "market_node": [
                    "AL00",
                    "AT00",
                    "BA00",
                    "BE00",
                    "BEO1_OFF",
                    "BG00",
                    "CH00",
                    "CY00",
                    "CZ00",
                    "DE00",
                    "DEKF_OFF",
                    "DKBH_OFF",
                    "DKE1",
                    "DKKF_OFF",
                    "DKNS_OFF",
                    "DKW1",
                    "EE00",
                    "ES00",
                    "FI00",
                    "FR00",
                    "GR00",
                    "GR03",
                    "HR00",
                    "HU00",
                    "IE00",
                    "ITCA",
                    "ITCN",
                    "ITCS",
                    "ITN1",
                    "ITS1",
                    "ITSA",
                    "ITSI",
                    "LT00",
                    "LUG1",
                    "LUV1",
                    "LV00",
                    "ME00",
                    "MK00",
                    "MT00",
                    "NL00",
                    "NOM1",
                    "NON1",
                    "NOS1",
                    "NOS2",
                    "NOS3",
                    "PL00",
                    "PLE0",
                    "PLI0",
                    "PT00",
                    "RO00",
                    "RS00",
                    "SE01",
                    "SE02",
                    "SE03",
                    "SE04",
                    "SI00",
                    "SK00",
                    "UK00",
                    "UKNI",
                ],
                "code_antares": [
                    "AL",
                    "AT",
                    "BA",
                    "BE",
                    "BEo1",
                    "BG",
                    "CH",
                    "CY",
                    "CZ",
                    "DE",
                    "DEkf",
                    "DKbh",
                    "DKe",
                    "DKkf",
                    "DKns",
                    "DKw",
                    "EE",
                    "ES",
                    "FI",
                    "FR",
                    "GR",
                    "GR",
                    "HR",
                    "HU",
                    "IE",
                    "ITca",
                    "ITcn",
                    "ITcs",
                    "ITn",
                    "ITs",
                    "ITsar",
                    "ITsic",
                    "LT",
                    "LU",
                    "LU",
                    "LV",
                    "ME",
                    "MK",
                    "MT",
                    "NL",
                    "NOm",
                    "NOn",
                    "NOs",
                    "NOs",
                    "NOs",
                    "PL",
                    "PL",
                    "PL",
                    "PT",
                    "RO",
                    "RS",
                    "SE1",
                    "SE2",
                    "SE3",
                    "SE4",
                    "SI",
                    "SK",
                    "UKgb",
                    "UKni",
                ],
            }
        ),
        "PEAK_PARAMS": pd.DataFrame(
            {
                "hour": [1, 2],
                "period_hour": ["HC", "HP"],
                "month": [1, 2],
                "period_month": ["winter", "summer"],
            }
        ),
    }

    output_path = tmp_path / "MAIN_PARAMS.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return output_path


# TODO add data test for index + TS
## mock data LINKS
@pytest.fixture
def mock_links_data_csv(tmp_path: Path) -> Path:
    ## create one data frame by file

    # "Transfer Links.csv"
    # multi GRT minimal data
    df_tf_multi = pd.DataFrame(
        {
            "ZONE": ["FR", "FR", "BE", "BE"],
            "MARKET_ZONE_SOURCE": ["BE00", "BE00", "FR00", "FR00"],
            "MARKET_ZONE_DESTINATION": ["FR00", "FR00", "BE00", "BE00"],
            "TRANSFER_TYPE": ["NTC", "NTC", "NTC", "NTC"],
            "STUDY_SCENARIO": [
                "ERAA&TYNDP",
                "ERAA&TYNDP",
                "ERAA&TYNDP",
                "ERAA&TYNDP",
            ],
            "YEAR_VALID_START": [2031, 2024, 2032, 2025],
            "YEAR_VALID_END": [2050, 2030, 2050, 2031],
            "TRANSFER_TECHNOLOGY": ["HVAC", "HVAC", "HVAC", "HVAC"],
            "NTC_LIMIT_CAPACITY_STATIC": ["", "", 5300.0, 4300.0],
            "NTC_CURVE_ID": ["BE-FR_2031_2050", "BE-FR_2024_2030", "", ""],
            "NO_POLES": [2, 2, 1, 1],
            "FOR": ["", "", 0.06, 0.06],
            "COMPL": ["Yes", "Yes", "No", "No"],
            "FOR_DIRECTION": [
                "Bi-directional",
                "Bi-directional",
                "Uni-directional",
                "Uni-directional",
            ],
            "EXCHANGE_FLOW_CURVE_ID": ["", "", "", ""],
        }
    )

    # "NTCs Index.csv"
    df_index = pd.DataFrame(
        {
            "CURVE_UID": ["FR:NTCs_BE_FR_2024_2030", "FR:NTCs_BE_FR_2031_2050"],
            "ZONE": ["FR", "FR"],
            "ID": ["BE-FR_2031_2050", "BE-FR_2024_2030"],
            "LABEL": ["xxx", "xxx"],
            "COUNT": [1, 2],
        }
    )

    # "NTCs.csv"
    dates = pd.date_range(start="2023-01-01 00:00", end="2023-12-31 23:00", freq="h")

    df_ntc_ts = pd.DataFrame(
        {
            "MONTH": dates.month,
            "DAY": dates.day,
            "HOUR": dates.hour + 1,
            "FR:NTCs_BE_FR_2024_2030": pd.Series(range(1, 8760 + 1)),
            "FR:NTCs_BE_FR_2031_2050": pd.Series(range(1, 8760 + 1)),
        }
    )

    # dictionary
    data_test = {
        "Transfer Links": df_tf_multi,
        "NTCs Index": df_index,
        "NTCs": df_ntc_ts,
    }

    output_path = tmp_path / "links_data"
    os.makedirs(output_path)

    for file_name in data_test.keys():
        df = data_test[file_name]
        file_name = file_name + ".csv"
        path_file = output_path / file_name
        df.to_csv(path_file, index=False)

    return output_path


## mock dictionnary of data frames for format test
@pytest.fixture
def mock_links_dict_data_frames() -> dict[str, pd.DataFrame]:
    data_links_managed = {
        "2030": pd.DataFrame(
            {
                "ZONE": ["GR", "GR", "RS", "RS"],
                "MARKET_ZONE_SOURCE": ["AL00", "GR00", "AL00", "RS00"],
                "MARKET_ZONE_DESTINATION": ["GR00", "AL00", "RS00", "AL00"],
                "TRANSFER_TYPE": ["NTC", "NTC", "NTC", "NTC"],
                "STUDY_SCENARIO": ["ERAA", "ERAA", "ERAA&TYNDP", "ERAA&TYNDP"],
                "YEAR_VALID_START": [2030, 2030, 2030, 2030],
                "YEAR_VALID_END": [2100, 2100, 2100, 2100],
                "TRANSFER_TECHNOLOGY": ["HVAC", "HVAC", "HVAC", "HVAC"],
                "NTC_LIMIT_CAPACITY_STATIC": [600.0, 600.0, pd.NA, pd.NA],
                "NTC_CURVE_ID": [pd.NA, pd.NA, "AL00-RS00-2", "RS00-AL00-2"],
                "NO_POLES": [2, 2, 2, 2],
                "FOR": [0.0, 0.0, pd.NA, pd.NA],
                "COMPL": ["Yes", "Yes", "Yes", "Yes"],
                "FOR_DIRECTION": [
                    "Bi-directional",
                    "Bi-directional",
                    "Bi-directional",
                    "Bi-directional",
                ],
                "EXCHANGE_FLOW_CURVE_ID": [pd.NA, pd.NA, pd.NA, pd.NA],
                "SUMMER_HC": [600.0, 600.0, 500.0, 500.0],
                "WINTER_HC": [600.0, 600.0, 500.0, 500.0],
                "SUMMER_HP": [600.0, 600.0, 500.0, 500.0],
                "WINTER_HP": [600.0, 600.0, 500.0, 500.0],
                "MEDIAN": [pd.NA, pd.NA, 500.0, 500.0],
                "code_source": ["AL", "GR", "AL", "RS"],
                "code_destination": ["GR", "AL", "RS", "AL"],
                "border": ["AL-GR", "GR-AL", "AL-RS", "RS-AL"],
            }
        ),
        "2040": pd.DataFrame(
            {
                "ZONE": ["GR", "GR", "RS", "RS"],
                "MARKET_ZONE_SOURCE": ["AL00", "GR00", "AL00", "RS00"],
                "MARKET_ZONE_DESTINATION": ["GR00", "AL00", "RS00", "AL00"],
                "TRANSFER_TYPE": ["NTC", "NTC", "NTC", "NTC"],
                "STUDY_SCENARIO": ["ERAA", "ERAA", "ERAA&TYNDP", "ERAA&TYNDP"],
                "YEAR_VALID_START": [2030, 2030, 2030, 2030],
                "YEAR_VALID_END": [2100, 2100, 2100, 2100],
                "TRANSFER_TECHNOLOGY": ["HVAC", "HVAC", "HVAC", "HVAC"],
                "NTC_LIMIT_CAPACITY_STATIC": [600.0, 600.0, pd.NA, pd.NA],
                "NTC_CURVE_ID": [pd.NA, pd.NA, "AL00-RS00-2", "RS00-AL00-2"],
                "NO_POLES": [2, 2, 2, 2],
                "FOR": [0.0, 0.0, pd.NA, pd.NA],
                "COMPL": ["Yes", "Yes", "Yes", "Yes"],
                "FOR_DIRECTION": [
                    "Bi-directional",
                    "Bi-directional",
                    "Bi-directional",
                    "Bi-directional",
                ],
                "EXCHANGE_FLOW_CURVE_ID": [pd.NA, pd.NA, pd.NA, pd.NA],
                "SUMMER_HC": [600.0, 600.0, 500.0, 500.0],
                "WINTER_HC": [600.0, 600.0, 500.0, 500.0],
                "SUMMER_HP": [600.0, 600.0, 500.0, 500.0],
                "WINTER_HP": [600.0, 600.0, 500.0, 500.0],
                "MEDIAN": [pd.NA, pd.NA, 500.0, 500.0],
                "code_source": ["AL", "GR", "AL", "RS"],
                "code_destination": ["GR", "AL", "RS", "AL"],
                "border": ["AL-GR", "GR-AL", "AL-RS", "RS-AL"],
            }
        ),
    }

    return data_links_managed


##
# data management tests
##


def test_links_files_not_exist(tmp_path: Path) -> None:
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )
    # then
    with pytest.raises(ValueError, match=re.escape("Input file does not exist: ")):
        links.links_data_management(conf_input=local_conf)


# test with mocked data to simulate no data with margin year
# for 2060 scenario ref => "TYNDP" => no DATA
def test_links_no_data_year(
    mock_links_data_csv: Path, mock_links_main_params_xlsx: Path
) -> None:
    # given
    year_param = [2030, 2060]
    local_conf = LocalConfiguration(
        input_path=mock_links_data_csv,
        export_path=mock_links_data_csv,
        scenario_name="test",
        data_references_path=mock_links_main_params_xlsx,
        calendar_year=year_param,
    )

    # when
    result = links.links_data_management(conf_input=local_conf)

    # then
    assert result["2030"].shape[0] > 0
    assert result["2060"].shape[0] == 0


# with all DATA parquet
def test_links_data_management_works(
    mock_links_main_params_xlsx: Path, parquet_to_csv: Path
) -> None:
    # given
    year_param = [2030, 2060]
    local_conf = LocalConfiguration(
        input_path=parquet_to_csv,
        export_path=parquet_to_csv,
        scenario_name="test",
        data_references_path=mock_links_main_params_xlsx,
        calendar_year=year_param,
    )

    # when
    result = links.links_data_management(conf_input=local_conf)

    # then
    assert result.keys() == {str(y) for y in year_param}
    assert all(isinstance(result[str(y)], pd.DataFrame) for y in year_param)


##
# data pegase format tests
##


def test_links_output_format_with_no_data() -> None:
    # given
    empty_dict: dict[str, pd.DataFrame] = {}
    # then
    with pytest.raises(ValueError, match="No DATA for export"):
        links.links_manage_output_format(data_dict=empty_dict)


def test_links_output_format_works(
    mock_links_dict_data_frames: dict[str, pd.DataFrame],
) -> None:
    # given
    df_test = mock_links_dict_data_frames
    # when
    final_dict_result = links.links_manage_output_format(data_dict=df_test)

    # then
    # type
    assert isinstance(final_dict_result, dict)
    # type keys
    assert all(isinstance(k, str) for k in df_test.keys())
    # named keys matching
    assert set(final_dict_result.keys()) == set(df_test.keys())
    # type content
    assert all(isinstance(v, pd.DataFrame) for v in final_dict_result.values())
    # col names order
    Col = conf_links.ExportLinksColumnsNames
    expected_cols = [c.value for c in Col]

    for df in final_dict_result.values():
        assert list(df.columns) == expected_cols


##
# exports processing
##


def test_links_manage_export_empty_data(tmp_path: Path) -> None:
    # then
    with pytest.raises(ValueError, match="No DATA to export"):
        links_manage_export(dict_of_df=dict({}), root_dir_export=tmp_path)


def test_links_manage_export_root_dir_not_exist(tmp_path: Path) -> None:
    # given
    fake_dir_path = tmp_path / "not_exist"
    df_minimal = {"2030": pd.DataFrame({"ZONE": ["FR", "FR"]})}
    # then
    with pytest.raises(
        ValueError,
        match=re.escape(f"Path of root directory {fake_dir_path} does not exist"),
    ):
        links_manage_export(dict_of_df=df_minimal, root_dir_export=fake_dir_path)


def test_links_manage_export_works(tmp_path: Path) -> None:
    # given
    columns_export = ExportLinksColumnsNames
    df_test = pd.DataFrame(
        {
            columns_export.NAME.value: ["AL-GR", "AL-RS"],
            columns_export.WINTER_HP_DIRECT_MW.value: [600.0, 500.0],
            columns_export.WINTER_HP_INDIRECT_MW.value: [600.0, 500.0],
            columns_export.WINTER_HC_DIRECT_MW.value: [600.0, 500.0],
            columns_export.WINTER_HC_INDIRECT_MW.value: [600.0, 500.0],
            columns_export.SUMMER_HP_DIRECT_MW.value: [600.0, 500.0],
            columns_export.SUMMER_HP_INDIRECT_MW.value: [600.0, 500.0],
            columns_export.SUMMER_HC_DIRECT_MW.value: [600.0, 500.0],
            columns_export.SUMMER_HC_INDIRECT_MW.value: [600.0, 500.0],
            columns_export.FLOWBASED_PERIMETER.value: [False, False],
            columns_export.HVDC_DIRECT.value: [None, None],
            columns_export.HVDC_INDIRECT.value: [None, None],
            columns_export.SPECIFIC_TS.value: [False, False],
            columns_export.FORCED_OUTAGE_HVAC.value: [False, False],
        }
    )

    dict_of_export_df = {"2030": df_test, "2040": df_test}

    links_manage_export(
        dict_of_df=dict_of_export_df,
        root_dir_export=tmp_path,
        scenario_name="export_test",
    )
