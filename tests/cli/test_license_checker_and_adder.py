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
from typing import List

from antares_data_collection.cli import license_checker_and_adder as lca

LICENSE_HEADER = lca.LICENSE_HEADER


# -------------------
# Fixture: single temporary Python file
# -------------------
@pytest.fixture
def tmp_py_file(tmp_path: Path) -> Path:
    file_path: Path = tmp_path / "test.py"
    file_path.write_text("print('hello world')\n")
    return file_path


# -------------------
# Fixture: temporary directory with several Python files
# -------------------
@pytest.fixture
def tmp_dir_with_files(tmp_path: Path) -> Path:
    # file without license header
    file1: Path = tmp_path / "a.py"
    file1.write_text("print('a')\n")

    # file with license header
    file2: Path = tmp_path / "b.py"
    file2.write_text(LICENSE_HEADER + "\nprint('b')\n")

    # file in a subdirectory
    subdir: Path = tmp_path / "subdir"
    subdir.mkdir()
    file3: Path = subdir / "c.py"
    file3.write_text("print('c')\n")

    return tmp_path


# -------------------
# Tests: check_file
# -------------------
def test_check_file_no_header(tmp_py_file: Path) -> None:
    result: bool = lca.check_file(tmp_py_file, action="check")
    assert result is False


def test_check_file_fix(tmp_py_file: Path) -> None:
    result: bool = lca.check_file(tmp_py_file, action="fix")
    assert result is False
    content: str = tmp_py_file.read_text()
    assert LICENSE_HEADER.splitlines()[0] in content


def test_check_file_already_licensed(tmp_py_file: Path) -> None:
    tmp_py_file.write_text(LICENSE_HEADER + "\nprint('hello')")
    result: bool = lca.check_file(tmp_py_file, action="check")
    assert result is True


# -------------------
# Tests: check_dir
# -------------------
def test_check_dir(tmp_dir_with_files: Path) -> None:
    invalid_files: List[Path] = []
    lca.check_dir(
        tmp_dir_with_files,
        tmp_dir_with_files,
        action="check",
        invalid_files=invalid_files,
    )
    a_path: Path = tmp_dir_with_files / "a.py"
    c_path: Path = tmp_dir_with_files / "subdir" / "c.py"
    # files without license header
    assert a_path in invalid_files
    assert c_path in invalid_files
    # file with license header
    b_path: Path = tmp_dir_with_files / "b.py"
    assert b_path not in invalid_files


def test_check_dir_fix(tmp_dir_with_files: Path) -> None:
    invalid_files: List[Path] = []
    lca.check_dir(
        tmp_dir_with_files,
        tmp_dir_with_files,
        action="fix",
        invalid_files=invalid_files,
    )
    # all files without header now have the license header
    a_path: Path = tmp_dir_with_files / "a.py"
    content_a: str = a_path.read_text()
    assert LICENSE_HEADER.splitlines()[0] in content_a

    c_path: Path = tmp_dir_with_files / "subdir" / "c.py"
    content_c: str = c_path.read_text()
    assert LICENSE_HEADER.splitlines()[0] in content_c
