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

from click.testing import CliRunner

from antares.data_collection.cli import license_checker_and_adder as lca

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


def test_check_file_fix_already_licensed(tmp_path: Path) -> None:
    # file with incomplete license header
    file3: Path = tmp_path / "bb.py"
    file3.write_text(
        "# Copyright (c) 2024, RTE (https://www.rte-france.com)" + "\nprint('bb')\n"
    )

    # then
    with pytest.raises(ValueError, match="already licensed."):
        # when
        lca.check_file(file3, action="fix")


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


# -------------------
# Tests: runner CliRunner()
# -------------------
def test_cli_raise_error_parameter(tmp_dir_with_files: Path) -> None:
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(lca.cli, [f"--path={tmp_dir_with_files}", "--action=toto"])

    # exit code
    assert result.exit_code != 0
    # check if ValueError
    assert isinstance(result.exception, ValueError)
    # catch ValueError message
    assert (
        "Parameter --action should be 'check', 'check-strict' or 'fix' and was 'toto'"
        in str(result.exception)
    )


def test_cli_all_good(tmp_path: Path) -> None:
    file = tmp_path / "good.py"
    file.write_text(lca.LICENSE_HEADER + "\nprint('ok')\n")

    runner = CliRunner()
    result = runner.invoke(lca.cli, [f"--path={tmp_path}", "--action=check"])
    assert result.exit_code == 0
    assert "All good !" in result.output


def test_cli_invalid_files_check(tmp_dir_with_files: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(lca.cli, [f"--path={tmp_dir_with_files}", "--action=check"])
    assert result.exit_code == 0
    assert "files have an invalid header" in result.output


def test_cli_invalid_files_check_strict(tmp_dir_with_files: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        lca.cli, [f"--path={tmp_dir_with_files}", "--action=check-strict"]
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "Some files have invalid headers" in str(result.exception)


def test_cli_invalid_files_fix(tmp_dir_with_files: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(lca.cli, [f"--path={tmp_dir_with_files}", "--action=fix"])
    assert result.exit_code == 0
    # le message confirme que les fichiers ont été fixés
    assert "files have been fixed" in result.output
    # vérifier qu'au moins un fichier a maintenant le header
    for f in tmp_dir_with_files.glob("*.py"):
        content = f.read_text()
        assert content.startswith("# Copyright")
