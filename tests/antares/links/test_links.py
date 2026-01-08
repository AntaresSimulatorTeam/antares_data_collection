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

from antares.data_collection.links import links
from antares.data_collection.tools.conf import LocalConfiguration

# global
ROOT_TEST = Path(__file__).resolve().parents[2]


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
                "YEAR": ["2030", "2060"],
                "STUDY_SCENARIO": ["ERAA", "TYNDP"],
            }
        ),
        "LINKS": pd.DataFrame(
            {
                "market_node": ["AL00", "AT00", "BE00", "FR00"],
                "code_antares": ["AL", "AT", "BE", "FR"],
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
    df_tf = pd.DataFrame(
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
    data_test = {"Transfer Links": df_tf, "NTCs Index": df_index, "NTCs": df_ntc_ts}

    output_path = tmp_path / "links_data"
    os.makedirs(output_path)

    for file_name in data_test.keys():
        df = data_test[file_name]
        file_name = file_name + ".csv"
        path_file = output_path / file_name
        df.to_csv(path_file, index=False)

    return output_path


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


##
# data management tests
##


def test_links_read_data(
    mock_links_main_params_xlsx: Path, mock_links_data_csv: Path
) -> None:
    local_conf = LocalConfiguration(
        input_path=mock_links_data_csv,
        export_path=mock_links_data_csv,
        scenario_name="test",
        data_references_path=mock_links_main_params_xlsx,
        calendar_year=[2030, 2060],
    )
    links.links_data_management(conf_input=local_conf)
