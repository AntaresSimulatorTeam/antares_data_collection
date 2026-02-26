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
from zipfile import ZipFile

import pandas as pd

from antares.data_collection import LocalConfiguration
from antares.data_collection.referential_data.main_params import (
    ClusterColumnsNames,
    CountryColumnsNames,
)
from antares.data_collection.thermal.conf_thermal import ThermalDataColumns
from antares.data_collection.thermal.thermal import (
    thermal_compute_power_number_capacity,
    thermal_import,
    thermal_pre_treatments,
    thermal_treatments_year,
)
from tests.conftest import RESOURCE_PATH


## mock referential MAIN_PARAMS EXCEL
@pytest.fixture
def mock_thermal_main_params_xlsx(tmp_path: Path) -> Path:
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
                "YEAR": ["2030", "2040", "2060", "2200"],
                "STUDY_SCENARIO": ["ERAA", "ERAA", "TYNDP", "TYNDP"],
            }
        ),
        "CLUSTER": pd.DataFrame(
            {
                "TYPE": ["Thermal", "Thermal", "Thermal"],
                "CLUSTER_PEMMDB": [
                    "Gas/CCGT CCS",
                    "OtherNon-RES/Gas/CCGT CCS",
                    "Gas/CCGT CCS/CHP",
                ],
                "CLUSTER_BP": ["CCGT CCS", "CCGT CCS", "CCGT CCS CHP"],
            }
        ),
    }

    output_path = tmp_path / "MAIN_PARAMS.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return output_path


## mock DATA thermal pre treatment (according to the referential above)
@pytest.fixture
def mock_thermal_data_pre_treatment_df() -> pd.DataFrame:
    df_thermal_pre_treat = pd.DataFrame(
        {
            "ZONE": ["AL", "AT"],
            "STUDY_SCENARIO": ["ERAA&TYNDP", "ERAA&TYNDP"],
            "MARKET_NODE": ["AL00", "AT00"],
            "COMMISSIONING_DATE": ["2020-01-01", "2020-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2040-01-01", "2040-01-01"],
            "OP_STAT": ["None", "Available on market"],
            "SCND_FUEL": ["Heavy oil", "Natural gas"],
            "SCND_FUEL_RT": [0, 0],
            "NET_MAX_GEN_CAP": [100, 120],
            "PEMMDB_TECHNOLOGY": ["Gas/CCGT CCS", "OtherNon-RES/Gas/CCGT CCS"],
        }
    )

    df_thermal_pre_treat_bio = pd.DataFrame(
        {
            "ZONE": ["BE"],
            "STUDY_SCENARIO": ["TYNDP"],
            "MARKET_NODE": ["BE00"],
            "COMMISSIONING_DATE": ["2020-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2040-01-01"],
            "OP_STAT": ["Available on market"],
            "SCND_FUEL": ["Bio"],
            "SCND_FUEL_RT": [0.5],
            "NET_MAX_GEN_CAP": [100],
            "PEMMDB_TECHNOLOGY": ["Gas/CCGT CCS/CHP"],
        }
    )

    df_thermal_pre_treat_no_code = pd.DataFrame(
        {
            "ZONE": ["BZZ"],
            "STUDY_SCENARIO": ["TYNDP"],
            "MARKET_NODE": ["BZZ00"],
            "COMMISSIONING_DATE": ["2020-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2040-01-01"],
            "OP_STAT": ["Available on market"],
            "SCND_FUEL": ["Bio"],
            "SCND_FUEL_RT": [0.5],
            "NET_MAX_GEN_CAP": [100],
            "PEMMDB_TECHNOLOGY": ["Gas/CCGT CCS"],
        }
    )

    df_thermal_pre_treat_full = pd.concat(
        [df_thermal_pre_treat, df_thermal_pre_treat_bio, df_thermal_pre_treat_no_code]
    )

    return df_thermal_pre_treat_full


## mock referential MAIN_PARAMS/PAYS Data Frame
@pytest.fixture
def mock_thermal_ref_pays() -> pd.DataFrame:
    ref_pays = pd.DataFrame(
        {
            "Nom_pays": ["Albanie", "Autriche", "Belgique", "France"],
            "code_pays": ["AL", "AT", "BE", "FR"],
            "areas": ["Albanie", "Autriche", "Belgique", "France"],
            "market_node": ["AL00", "AT00", "BE00", "FR00"],
            "code_antares": ["AL", "AT", "BE", "FR"],
        }
    )

    return ref_pays


## mock referential MAIN_PARAMS/CLUSTER Data Frame
@pytest.fixture
def mock_thermal_ref_cluster() -> pd.DataFrame:
    ref_cluster = pd.DataFrame(
        {
            "TYPE": ["Thermal", "Thermal", "Thermal"],
            "CLUSTER_PEMMDB": [
                "Gas/CCGT CCS",
                "OtherNon-RES/Gas/CCGT CCS",
                "Gas/CCGT CCS/CHP",
            ],
            "CLUSTER_BP": ["CCGT CCS", "CCGT CCS", "CCGT CCS CHP"],
        }
    )

    return ref_cluster


## mock referential MAIN_PARAMS/STUDY_SCENARIO Data Frame
@pytest.fixture
def mock_thermal_ref_study_scenario() -> pd.DataFrame:
    ref_study_scenario = pd.DataFrame(
        {
            "YEAR": ["2030", "2040", "2060", "2200"],
            "STUDY_SCENARIO": ["ERAA", "ERAA", "TYNDP", "TYNDP"],
        }
    )
    return ref_study_scenario


## mock DATA thermal pre treated (with cluster bio, code_antares, cluster_bp)
@pytest.fixture
def mock_thermal_data_pre_treated_df() -> pd.DataFrame:
    # columns necessary: ['code_antares', 'STUDY_SCENARIO', 'COMMISSIONING_DATE',
    #        'DECOMMISSIONING_DATE_EXPECTED', 'CLUSTER_BP', 'NET_MAX_GEN_CAP']
    # values are compatible with value of mock

    list_code_antares = ["BE", "FR"]
    df_cluster_2030 = pd.DataFrame(
        {
            "STUDY_SCENARIO": ["ERAA&TYNDP", "ERAA", "TYNDP"],
            "COMMISSIONING_DATE": ["2020-01-01", "2020-01-01", "2020-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2030-01-01", "2030-01-01", "2030-01-01"],
            "CLUSTER_BP": ["CCGT CCS", "CCGT CCS", "CCGT CCS CHP"],
            "NET_MAX_GEN_CAP": [100, 120, 100],
        }
    )

    df_cluster_2040 = pd.DataFrame(
        {
            "STUDY_SCENARIO": ["ERAA&TYNDP", "ERAA", "TYNDP"],
            "COMMISSIONING_DATE": ["2031-01-01", "2031-01-01", "2031-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2040-01-01", "2040-01-01", "2040-01-01"],
            "CLUSTER_BP": ["CCGT CCS bio", "CCGT CCS bio", "CCGT CCS CHP"],
            "NET_MAX_GEN_CAP": [100, 120, 100],
        }
    )

    df_cluster_2060 = pd.DataFrame(
        {
            "STUDY_SCENARIO": ["ERAA&TYNDP", "ERAA", "TYNDP"],
            "COMMISSIONING_DATE": ["2051-01-01", "2051-01-01", "2051-01-01"],
            "DECOMMISSIONING_DATE_EXPECTED": ["2060-01-01", "2060-01-01", "2060-01-01"],
            "CLUSTER_BP": ["CCGT CCS bio", "CCGT CCS bio", "CCGT CCS CHP"],
            "NET_MAX_GEN_CAP": [100, 120, 100],
        }
    )

    concat_df = pd.concat([df_cluster_2030, df_cluster_2040, df_cluster_2060])

    df_pre_treated_full = pd.merge(pd.Series(list_code_antares, name="code_antares"), concat_df, how="cross")

    # convert to datetime
    cols_to_convert = [
        "COMMISSIONING_DATE",
        "DECOMMISSIONING_DATE_EXPECTED",
    ]

    df_pre_treated_full[cols_to_convert] = df_pre_treated_full[cols_to_convert].apply(pd.to_datetime)

    return df_pre_treated_full


##
# Import
##


def test_thermal_import_file_not_exists(tmp_path: Path) -> None:
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )
    # then
    with pytest.raises(ValueError, match=re.escape("Input file does not exist: ")):
        thermal_import(conf_input=local_conf)


def test_thermal_import_pandas_raise_usecols_parameters(tmp_path: Path) -> None:
    # given
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )
    path_file = local_conf.input_path / "Thermal.csv"
    df_empty = pd.DataFrame(columns=["ZONE", "Column 1", "Column 2", "Column 3"])
    df_empty.to_csv(path_file, index=False)

    # then (
    with pytest.raises(ValueError, match=re.escape("Usecols do not match columns, ")):
        thermal_import(conf_input=local_conf)


def test_thermal_import_empty_file(tmp_path: Path) -> None:
    # given
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )

    path_file = local_conf.input_path / "Thermal.csv"
    df_empty = pd.DataFrame(
        columns=[
            "ZONE",
            "STUDY_SCENARIO",
            "MARKET_NODE",
            "COMMISSIONING_DATE",
            "DECOMMISSIONING_DATE_EXPECTED",
            "OP_STAT",
            "SCND_FUEL",
            "SCND_FUEL_RT",
            "NET_MAX_GEN_CAP",
            "PEMMDB_TECHNOLOGY",
        ]
    )
    df_empty.to_csv(path_file, index=False)

    # then
    with pytest.raises(ValueError, match=re.escape(f"Input file is empty: {path_file}")):
        thermal_import(conf_input=local_conf)


def test_thermal_import_works(tmp_path: Path) -> None:
    # given
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )

    path_file = local_conf.input_path / "Thermal.csv"

    df_test = pd.DataFrame(
        {
            "ZONE": ["1"],
            "STUDY_SCENARIO": ["2"],
            "MARKET_NODE": ["3"],
            "COMMISSIONING_DATE": "2024-01-01",
            "DECOMMISSIONING_DATE_EXPECTED": "2030-12-31",
            "OP_STAT": ["6"],
            "SCND_FUEL": ["7"],
            "SCND_FUEL_RT": ["8"],
            "NET_MAX_GEN_CAP": ["9"],
            "PEMMDB_TECHNOLOGY": ["10"],
        }
    )

    df_test.to_csv(path_file, index=False)

    # when
    df_imported = thermal_import(conf_input=local_conf)
    # then
    assert isinstance(df_imported, pd.DataFrame)
    assert not df_imported.empty

    # type datetime for columns treated as date
    assert all(
        pd.api.types.is_datetime64_any_dtype(df_imported[col])
        for col in ["COMMISSIONING_DATE", "DECOMMISSIONING_DATE_EXPECTED"]
    )


def test_thermal_import_real_test_case(tmp_path: Path) -> None:
    # Use real test case
    file_path = RESOURCE_PATH / "data_thermal_installed_power" / "Thermal.zip"

    with ZipFile(file_path, "r") as z:
        z.extractall(tmp_path)

    # given
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )

    # when
    df_imported = thermal_import(conf_input=local_conf)

    # then
    list_cols_expected = [
        "ZONE",
        "STUDY_SCENARIO",
        "MARKET_NODE",
        "COMMISSIONING_DATE",
        "DECOMMISSIONING_DATE_EXPECTED",
        "OP_STAT",
        "SCND_FUEL",
        "SCND_FUEL_RT",
        "NET_MAX_GEN_CAP",
        "PEMMDB_TECHNOLOGY",
    ]
    assert isinstance(df_imported, pd.DataFrame)
    assert not df_imported.empty
    assert list(df_imported.columns) == list_cols_expected

    # type datetime for columns treated as date
    assert all(
        pd.api.types.is_datetime64_any_dtype(df_imported[col])
        for col in ["COMMISSIONING_DATE", "DECOMMISSIONING_DATE_EXPECTED"]
    )


##
# pre treatments
##


def test_thermal_pre_treatments_default_works(
    mock_thermal_data_pre_treatment_df: pd.DataFrame,
    mock_thermal_ref_pays: pd.DataFrame,
    mock_thermal_ref_cluster: pd.DataFrame,
) -> None:
    # given
    ref_pays = mock_thermal_ref_pays
    ref_cluster = mock_thermal_ref_cluster

    # when
    df_pre_treat = thermal_pre_treatments(
        df_thermal=mock_thermal_data_pre_treatment_df,
        df_ref_pays=ref_pays,
        df_ref_cluster=ref_cluster,
    )

    list_cols_expected = [
        CountryColumnsNames.CODE_ANTARES.value,
        ThermalDataColumns.STUDY_SCENARIO.value,
        ThermalDataColumns.COMMISSIONING_DATE.value,
        ThermalDataColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        ClusterColumnsNames.CLUSTER_BP.value,
        ThermalDataColumns.NET_MAX_GEN_CAP.value,
    ]

    assert isinstance(df_pre_treat, pd.DataFrame)
    assert list_cols_expected == list(df_pre_treat.columns)
    assert not df_pre_treat.empty

    # test columns CLUSTER_BP is updated with line containing bio
    assert df_pre_treat.loc[df_pre_treat.code_antares == "BE", "CLUSTER_BP"].iloc[0] == "CCGT CCS CHP bio"


def test_thermal_pre_treatments_real_test_case(tmp_path: Path) -> None:
    # Use real main_params file
    file_path_main_params = RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx"

    # use imported data from real file "Thermal.csv"
    file_path_data = RESOURCE_PATH / "data_thermal_installed_power" / "thermal_imported.zip"

    with ZipFile(file_path_data, "r") as z:
        z.extractall(tmp_path)

    file_path_data = tmp_path / "thermal_imported.csv"

    # given
    df_data = pd.read_csv(file_path_data)
    df_ref_pays = pd.read_excel(file_path_main_params, sheet_name="PAYS")
    df_ref_cluster = pd.read_excel(file_path_main_params, sheet_name="CLUSTER")

    # when
    df_pre_treated = thermal_pre_treatments(
        df_thermal=df_data,
        df_ref_pays=df_ref_pays,
        df_ref_cluster=df_ref_cluster,
    )

    assert isinstance(df_pre_treated, pd.DataFrame)


##
# treatments by year
##


def test_thermal_compute_power_number_capacity() -> None:
    # given
    df_test = pd.DataFrame(
        {
            "col_index_1": ["A", "A", "B"],
            "col_index_2": ["C", "C", "D"],
            "numeric_col": [10, 20, 30],
            "col_not_used": ["toto", "titi", "tata"],
        }
    )

    # to test limit of "count" at max 100
    df_test = pd.concat([df_test] * 120, ignore_index=True)

    # when
    res = thermal_compute_power_number_capacity(
        df_input=df_test,
        name_cols_index=["col_index_1", "col_index_2"],
        name_capacity_col="numeric_col",
    )

    # then
    df_expected = pd.DataFrame(
        {
            "col_index_1": ["A", "B"],
            "col_index_2": ["C", "D"],
            "power": [3600, 3600],
            "number": [100, 100],
        }
    )

    pd.testing.assert_frame_equal(df_expected, res)


def test_thermal_treatments_by_year_2030_eraa(
    mock_thermal_data_pre_treated_df: pd.DataFrame,
) -> None:
    # given
    year_tested = pd.Timestamp("2030-01-01")
    filter_scenario_input_value = "ERAA"
    df_test = mock_thermal_data_pre_treated_df

    # when
    df_treated = thermal_treatments_year(
        df_thermal_pre_treated=df_test,
        year_input=year_tested,
        filter_scenario_input=filter_scenario_input_value,
    )

    assert isinstance(df_treated, pd.DataFrame)

    df_expected = pd.DataFrame(
        {
            "code_antares": ["BE", "FR"],
            "CLUSTER_BP": ["CCGT CCS", "CCGT CCS"],
            "power": [220, 220],
            "number": [2, 2],
        }
    )
    pd.testing.assert_frame_equal(df_expected, df_treated)


def test_thermal_treatments_by_year_2040(
    mock_thermal_data_pre_treated_df: pd.DataFrame,
) -> None:
    # given
    year_tested = pd.Timestamp("2040-01-01")
    filter_scenario_input_value = "ERAA"
    df_test = mock_thermal_data_pre_treated_df

    # when
    df_treated = thermal_treatments_year(
        df_thermal_pre_treated=df_test,
        year_input=year_tested,
        filter_scenario_input=filter_scenario_input_value,
    )

    # then
    assert isinstance(df_treated, pd.DataFrame)
    df_expected = pd.DataFrame(
        {
            "code_antares": ["BE", "FR"],
            "CLUSTER_BP": ["CCGT CCS bio", "CCGT CCS bio"],
            "power": [220, 220],
            "number": [2, 2],
        }
    )
    pd.testing.assert_frame_equal(df_expected, df_treated)
