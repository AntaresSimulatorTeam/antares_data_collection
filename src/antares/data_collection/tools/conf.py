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

from pathlib import Path


class LocalConfiguration:
    def __init__(
        self,
        input_path: Path,
        export_path: Path,
        scenario_name: str,
        data_references_path: Path,
        calendar_year: list[int] = [2030, 2060],
    ):
        self.input_path = Path(input_path)
        self.export_path = Path(export_path)
        self.scenario_name = scenario_name
        self.data_references_path = Path(data_references_path)
        self.calendar_year = calendar_year

        self._validate()

    def _validate(self) -> None:
        if not self.input_path.exists():
            raise FileNotFoundError(
                f"Input directory does not exist: {self.input_path}"
            )

        if not self.export_path.exists():
            raise FileNotFoundError(
                f"Export directory does not exist: {self.export_path}"
            )

        if not self.scenario_name:
            raise ValueError("Scenario name cannot be empty")

        if not self.data_references_path.exists():
            raise FileNotFoundError(
                f"Data references files does not exist: {self.data_references_path}"
            )
