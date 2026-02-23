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

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams


def test_main_params_file_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"Input file does not exist: {fake_path}"),
    ):
        MainParams(path_file=fake_path)


def test_main_params_default_sheet_names(tmp_path: Path) -> None:
    # given
    path_file = tmp_path / "MAIN_PARAMS.xlsx"
    pd.DataFrame({"A": [1, 2, 3]}).to_excel(path_file, sheet_name="test")

    # then
    with pytest.raises(
        ValueError,
        match=re.escape("Sheet 'test' not found in MAIN_PARAMS.xlsx"),
    ):
        MainParams(path_file=path_file)


def test_main_params_with_bad_name_columns(tmp_path: Path) -> None:
    # given
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    ref_pays = pd.DataFrame(
        {
            "Nom_pays": ["ok"],
            "code_pays": ["ok"],
            "areas": ["ok"],
            "market_node": ["ok"],
            # ,
            # "code_antares": ["ok"]
        }
    )

    ref_scenario = pd.DataFrame({"YEAR": ["ok"], "STUDY_SCENARIO": ["ok"]})

    ref_cluster = pd.DataFrame({"TYPE": ["ok"], "CLUSTER_PEMMDB": ["ok"], "CLUSTER_BP": ["ok"]})

    ref_peak_params = pd.DataFrame({"hour": ["ok"], "period_hour": ["ok"], "month": ["ok"], "period_month": ["ok"]})

    # write test workbook
    ref_pays.to_excel(path_file, sheet_name="PAYS", index=False)

    with pd.ExcelWriter(path_file, engine="openpyxl", mode="a") as writer:
        ref_scenario.to_excel(writer, sheet_name="STUDY_SCENARIO", index=False)
        ref_cluster.to_excel(writer, sheet_name="CLUSTER", index=False)
        ref_peak_params.to_excel(writer, sheet_name="PEAK_PARAMS", index=False)

    # then
    with pytest.raises(
        ValueError,
        match=re.escape("Columns names mismatch for sheet 'PAYS'"),
    ):
        MainParams(path_file=path_file)


def test_main_params_works_one_ref(tmp_path: Path) -> None:
    # given
    path_file = tmp_path / "MAIN_PARAMS.xlsx"

    ref_pays = pd.DataFrame(
        {"Nom_pays": ["ok"], "code_pays": ["ok"], "areas": ["ok"], "market_node": ["ok"], "code_antares": ["ok"]}
    )

    # write test workbook
    ref_pays.to_excel(path_file, sheet_name="PAYS", index=False)

    # then works
    MainParams(path_file=path_file, sheets_name=["PAYS"])
