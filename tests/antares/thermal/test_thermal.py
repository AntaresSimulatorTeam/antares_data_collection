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
from antares.data_collection.thermal.thermal import thermal_import


## mock referential MAIN_PARAMS
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


# ## mock DATA thermal (according to the referential above)
# @pytest.fixture
# def mock_thermal_data_csv(tmp_path: Path) -> Path:
#
#     output_path = tmp_path / "thermal_data_test"
#     os.makedirs(output_path)
#
#     return tmp_path / "thermal_data.csv"


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
