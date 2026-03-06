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

from pathlib import Path

import pandas as pd

RESOURCE_PATH = Path(__file__).parent / "antares" / "resources"
INPUT_RESOURCE_PATH = Path(__file__).parent / "antares" / "resources" / "input_files"
OUTPUT_RESOURCE_PATH = Path(__file__).parent / "antares" / "resources" / "expected_output_files"


## mock referential MAIN_PARAMS (used for links)
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
                "YEAR": ["2030", "2040", "2060", "2200"],
                "STUDY_SCENARIO": ["ERAA", "ERAA", "TYNDP", "TYNDP"],
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
