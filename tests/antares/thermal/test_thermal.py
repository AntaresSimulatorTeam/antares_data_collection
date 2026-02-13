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
import re

import numpy as np
import pandas as pd
import pytest

from antares.data_collection import LocalConfiguration
from antares.data_collection.referential_data.struct_main_params import (
    CountryColumnsNames,
    ClusterColumnsNames,
)
from antares.data_collection.thermal.conf_thermal import ThermalDataColumns
from antares.data_collection.thermal.thermal import (
    thermal_import,
    thermal_pre_treatments,
)


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
            "DECOMMISSIONING_DATE_OFFICIAL": ["2020-01-01", "2020-01-01"],
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
            "DECOMMISSIONING_DATE_OFFICIAL": ["2020-01-01"],
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
            "DECOMMISSIONING_DATE_OFFICIAL": ["2020-01-01"],
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
            "DECOMMISSIONING_DATE_OFFICIAL",
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
    with pytest.raises(
        ValueError, match=re.escape(f"Input file is empty: {path_file}")
    ):
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
        data=np.array([["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]]),
        columns=[
            "ZONE",
            "STUDY_SCENARIO",
            "MARKET_NODE",
            "DECOMMISSIONING_DATE_OFFICIAL",
            "DECOMMISSIONING_DATE_EXPECTED",
            "OP_STAT",
            "SCND_FUEL",
            "SCND_FUEL_RT",
            "NET_MAX_GEN_CAP",
            "PEMMDB_TECHNOLOGY",
        ],
    )
    df_test.to_csv(path_file, index=False)

    # when
    df_imported = thermal_import(conf_input=local_conf)
    # then
    assert isinstance(df_imported, pd.DataFrame)
    assert not df_imported.empty


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
        ThermalDataColumns.DECOMMISSIONING_DATE_OFFICIAL.value,
        ThermalDataColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        ClusterColumnsNames.CLUSTER_BP.value,
        ThermalDataColumns.NET_MAX_GEN_CAP.value,
    ]

    assert isinstance(df_pre_treat, pd.DataFrame)
    assert list_cols_expected == list(df_pre_treat.columns)
    assert not df_pre_treat.empty
