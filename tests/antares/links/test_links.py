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

from antares.data_collection.links import conf_links, links
from antares.data_collection.tools.conf import LocalConfiguration

# global
ROOT_TEST = Path(__file__).resolve().parents[2]
LINKS_DATA_DIR = ROOT_TEST / "antares" / "links" / "data_test"
REF_DATA = ROOT_TEST / "data_references"


# -------------------
# Fixture: temporary directory with fake links files
# -------------------
@pytest.fixture
def tmp_dir_with_links_files(tmp_path: Path) -> Path:
    conf_links_files = conf_links.LinksFileConfig()
    names_files = conf_links_files.all_names()
    for filename in names_files:
        file_path: Path = tmp_path / filename
        file_path.touch()

    return tmp_path


## mock referential MAIN_PARAMS
@pytest.fixture
def mock_links_main_params_xlsx(tmp_path: Path) -> Path:
    data = {
        "PAYS": pd.DataFrame(
            {
                "Nom_pays": ["Albanie", "Autriche"],
                "code_pays": ["AL", "AT"],
                "areas": ["Albanie", "Autriche"],
                "market_node": ["AL00", "AT00"],
                "code_antares": ["AL", "AT"],
            }
        ),
        "STUDY_SCENARIO": pd.DataFrame(
            {
                "YEAR": ["2030", "2060"],
                "STUDY_SCENARIO": ["ERAA", "TYNDP"],
            }
        ),
        "LINKS": pd.DataFrame(
            {
                "market_node": ["AL00", "AT00"],
                "code_antares": ["AL", "AT"],
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
    data_test = {
        "Transfer Links": pd.DataFrame(
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
        ),
    }

    output_path = tmp_path / "links_data"
    os.makedirs(output_path)

    for file_name in data_test.keys():
        df = data_test[file_name]
        file_name = file_name + ".csv"
        path_file = output_path / file_name
        df.to_csv(path_file, index=False)

    return output_path


def test_fixture(mock_links_data_csv: Path, mock_links_main_params_xlsx: Path) -> None:
    LocalConfiguration(
        input_path=mock_links_data_csv,
        export_path=mock_links_data_csv,
        scenario_name="test",
        data_references_path=mock_links_main_params_xlsx,
        calendar_year=[2030, 2060],
    )


def test_links_files_not_exist(tmp_path: Path) -> None:
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=tmp_path,
    )
    # then
    with pytest.raises(ValueError, match=re.escape("Input file does not exist: ")):
        links.create_links_part(conf_input=local_conf)


##
# data management tests
##


def test_links_read_data(mock_links_main_params_xlsx: Path) -> None:
    local_conf = LocalConfiguration(
        input_path=LINKS_DATA_DIR,
        export_path=LINKS_DATA_DIR,
        scenario_name="test",
        data_references_path=mock_links_main_params_xlsx,
        calendar_year=[2030, 2060],
    )
    links.create_links_part(conf_input=local_conf)
