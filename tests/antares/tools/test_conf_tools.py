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
import re

from antares.data_collection.tools.conf import LocalConfiguration


def test_conf_input_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"Input directory does not exist: {fake_path}"),
    ):
        LocalConfiguration(
            input_path=fake_path,
            export_path=fake_path,
            scenario_name="test",
            data_references_path=fake_path,
        )


def test_conf_output_not_exist(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"Export directory does not exist: {fake_path}"),
    ):
        LocalConfiguration(
            input_path=tmp_path,
            export_path=fake_path,
            scenario_name="test",
            data_references_path=fake_path,
        )


def test_conf_scenario_name_empty(tmp_path: Path) -> None:
    # then
    with pytest.raises(ValueError, match=re.escape("Scenario name cannot be empty")):
        LocalConfiguration(
            input_path=tmp_path,
            export_path=tmp_path,
            scenario_name="",
            data_references_path=tmp_path,
        )


def test_conf_ref_not_exist(tmp_path: Path) -> None:
    # given
    fake_path_file = tmp_path / "MAIN_PARAMS.xlsx"

    # then
    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"Data references files does not exist: {fake_path_file}"),
    ):
        LocalConfiguration(
            input_path=tmp_path,
            export_path=tmp_path,
            scenario_name="test",
            data_references_path=fake_path_file,
        )
