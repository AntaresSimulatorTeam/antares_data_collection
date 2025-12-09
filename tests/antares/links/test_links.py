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

from antares.data_collection.links import conf_links, links


# -------------------
# Fixture: temporary directory with fake links files
# -------------------
@pytest.fixture
def tmp_dir_with_links_files(tmp_path: Path) -> Path:
    names_files = conf_links.LinksFileNames().files
    for filename in names_files:
        file_path: Path = tmp_path / filename
        file_path.touch()

    return tmp_path


def test_links_dir_input_not_exists(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(
        ValueError, match=re.escape(f"Input directory {fake_path} does not exist.")
    ):
        links.create_links_part(dir_input=fake_path, dir_output=fake_path)


def test_links_dir_output_not_exists(tmp_path: Path) -> None:
    # given
    fake_path = tmp_path / "toto"

    # then
    with pytest.raises(
        ValueError, match=re.escape(f"Output directory {fake_path} does not exist.")
    ):
        links.create_links_part(dir_input=tmp_path, dir_output=fake_path)


def test_links_files_not_exist(tmp_path: Path) -> None:
    # when
    with pytest.raises(ValueError, match="Input file does not exist:"):
        links.create_links_part(dir_input=tmp_path, dir_output=tmp_path)


# def test_links_files_exist(tmp_dir_with_links_files: Path) -> None:
#     # when
#     links.create_links_part(
#         dir_input=tmp_dir_with_links_files, dir_output=tmp_dir_with_links_files
#     )

##
# data management tests
##

ROOT_TEST = Path(__file__).resolve().parents[2]  # ou 3 selon ta structure
DATA_DIR = ROOT_TEST / "antares" / "links" / "data_test"


def test_links_read_data() -> None:
    links.create_links_part(
        dir_input=DATA_DIR,
        dir_output=DATA_DIR,
    )
