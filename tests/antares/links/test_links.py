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


# TODO create test data usefull
# ## mock data LINKS
# @pytest.fixture
# def mock_links_data_csv(tmp_path: Path) -> Path:
#
#     df_transfert_links = {
#         "PAYS": pd.DataFrame(
#             {
#                 'ZONE': ['AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'AL', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR', 'GR'],
#                 'MARKET_ZONE_SOURCE': ['AL00', 'AL00', 'AL00', 'AL00', 'AL00', 'AL00', 'GR00', 'GR00', 'ME00', 'MK00', 'RS00', 'RS00', 'AL00', 'AL00', 'BG00', 'BG00', 'CY00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR03', 'GR03', 'GR03', 'ITS1', 'ITS1', 'MK00', 'MK00', 'TR00', 'TR00', 'TR00'],
#                 'MARKET_ZONE_DESTINATION': ['GR00', 'GR00', 'ME00', 'MK00', 'RS00', 'RS00', 'AL00', 'AL00', 'AL00', 'AL00', 'AL00', 'AL00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR03', 'AL00', 'AL00', 'BG00', 'BG00', 'EG00', 'GR03', 'GR03', 'ITS1', 'ITS1', 'MK00', 'MK00', 'TR00', 'TR00', 'TR00', 'CY00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00', 'GR00'],
#                 'TRANSFER_TYPE': ['NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'Exchange', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC', 'NTC'],
#                 'STUDY_SCENARIO': ['ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA&TYNDP', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA', 'ERAA'],
#                 'YEAR_VALID_START': [2030, 2026, 2026, 2026, 2030, 2026, 2026, 2030, 2026, 2026, 2026, 2030, 2030, 2024, 2024, 2032, 2030, 2030, 2024, 2032, 2024, 2033, 2026, 2025, 2033, 2024, 2028, 2024, 2033, 2028, 2024, 2030, 2025, 2026, 2024, 2033, 2028, 2024, 2033, 2024, 2028],
#                 'YEAR_VALID_END': [2100, 2029, 2100, 2100, 2100, 2029, 2029, 2100, 2100, 2100, 2029, 2100, 2100, 2029, 2031, 2100, 2100, 2100, 2029, 2100, 2031, 2100, 2100, 2025, 2100, 2032, 2100, 2027, 2100, 2032, 2027, 2100, 2025, 2100, 2032, 2100, 2100, 2027, 2100, 2027, 2032],
#                 'TRANSFER_TECHNOLOGY': ['HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVDC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', nan, 'HVAC', 'HVAC', 'HVDC', 'HVDC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVDC', 'HVAC', 'HVAC', 'HVDC', 'HVDC', 'HVAC', 'HVAC', 'HVAC', 'HVAC', 'HVAC'],
#                 'NTC_LIMIT_CAPACITY_STATIC': [650.0, 450.0, 400.0, 500.0, 500.0, 400.0, 450.0, 650.0, 400.0, 500.0, 400.0, 500.0, 600.0, 450.0, 1150.0, 1700.0, 1000.0, 600.0, 450.0, 1400.0, 1000.0, nan, 800.0, nan, 1500.0, 500.0, 1100.0, 650.0, 1260.0, 660.0, 218.0, 1000.0, nan, 800.0, 500.0, 1500.0, 850.0, 650.0, 1180.0, 166.0, 580.0],
#                 'NTC_CURVE_ID': [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, 'GR00_GR03_2025', nan, nan, nan, nan, nan, nan, nan, nan, 'GR00_GR03_2025', nan, nan, nan, nan, nan, nan, nan, nan],
#                 'NO_POLES': [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 3, 1, 2, 1, 2, 2, 1, 2, 2, 2, 1, 3, 2, 1, 2, 1, 2],
#                 'FOR': [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, 0.0, 0.0, 0.0, 0.0, 0.0005, 0.0, 0.0, 0.0, 0.0, 0.0016, 0.0, 0.0, 0.0436, 0.125, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0005, 0.0, 0.0, 0.125, 0.0436, 0.0, 0.0, 0.0, 0.0, 0.0],
#                 'COMPL': ['Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes'],
#                 'FOR_DIRECTION': ['Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional', 'Bi-directional'],
#                 'EXCHANGE_FLOW_CURVE_ID': [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, 'GR-EG', nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan]
#             }
#         ),
#     }
#
#     output_path=tmp_path / "links_data"
#     with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
#         for sheet_name, df in data.items():
#             df.to_excel(writer, sheet_name=sheet_name, index=False)
#
#     return output_path


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
