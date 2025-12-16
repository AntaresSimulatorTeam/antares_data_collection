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
from typing import Any

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


##
# all ref needed
##
@pytest.fixture
def ref_params() -> dict[str, Any]:
    ref_code_antares = pd.read_csv(REF_DATA / "ref_pays.csv")
    ref_links = pd.read_csv(REF_DATA / "ref_links.csv")
    ref_scenario = pd.read_csv(REF_DATA / "study_scenario.csv")
    ref_peak_hours = pd.read_csv(REF_DATA / "peak_hours.csv")
    ref_peak_months = pd.read_csv(REF_DATA / "peak_months.csv")
    year = list(range(2030, 2061))

    return {
        "ref_code_antares": ref_code_antares,
        "ref_links": ref_links,
        "ref_scenario": ref_scenario,
        "ref_peak_hours": ref_peak_hours,
        "ref_peak_months": ref_peak_months,
        "calendar_year": year,
    }


def test_links_files_not_exist(tmp_path: Path) -> None:
    local_conf = LocalConfiguration(
        input_path=tmp_path,
        export_path=tmp_path,
        scenario_name="test",
        data_references=tmp_path,
    )
    # then
    with pytest.raises(ValueError, match=re.escape("Input file does not exist: ")):
        links.create_links_part(conf_input=local_conf, useless="useless")


##
# data management tests
##

def test_links_read_data(ref_params: dict[str, pd.DataFrame]) -> None:
    local_conf = LocalConfiguration(
        input_path=LINKS_DATA_DIR,
        export_path=LINKS_DATA_DIR,
        scenario_name="test",
        data_references=REF_DATA,
    )
    links.create_links_part(conf_input=local_conf, ref_params=ref_params)


# TODO create .csv (temp) and delete then
