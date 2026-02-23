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

# structure Referential (MAIN_PARAMS.xlsx)
from enum import Enum
from pathlib import Path

import pandas as pd

from openpyxl.reader.excel import load_workbook


# all workbook sheet names
class ReferentialSheetNames(Enum):
    PAYS = "PAYS"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    LINKS = "LINKS"
    CLUSTER = "CLUSTER"
    PEAK_PARAMS = "PEAK_PARAMS"


# sheet "PAYS"
class CountryColumnsNames(Enum):
    NOM_PAYS = "Nom_pays"
    CODE_PAYS = "code_pays"
    AREAS = "areas"
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# sheet "STUDY_SCENARIO"
class StudyScenarioColumnsNames(Enum):
    YEAR = "YEAR"
    STUDY_SCENARIO = "STUDY_SCENARIO"


# sheet "LINKS"
class LinksColumnsNames(Enum):
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# sheet "CLUSTER"
class ClusterColumnsNames(Enum):
    TYPE = "TYPE"
    CLUSTER_PEMMDB = "CLUSTER_PEMMDB"
    CLUSTER_BP = "CLUSTER_BP"


# "PEAK_PARAMS"
class PeakParamsColumnsNames(Enum):
    HOUR = "hour"
    PERIOD_HOUR = "period_hour"
    MONTH = "month"
    PERIOD_MONTH = "period_month"


class MainParams:
    """
    Validates the structure and schema of a MAIN_PARAMS.xlsx workbook.

    This class enforces a strict data contract on an Excel file by checking:
    - The file exists at the provided path.
    - All required workbook sheets are present.
    - Each sheet contains exactly the expected columns, ignoring order.
      No extra columns are allowed.

    Attributes:
        path_file (Path): Path to the Excel file to validate.
        sheets_name (list[str]): List of expected sheet names.
            Defaults to `target_sheet_names`.

    Raises:
        FileNotFoundError: If the provided file path does not exist.
        ValueError: If:
            - A required sheet is missing.
            - A sheet's columns do not match the expected schema
              (including missing or unexpected columns).
    """

    target_sheet_names = [
        ReferentialSheetNames.PAYS.value,
        ReferentialSheetNames.STUDY_SCENARIO.value,
        ReferentialSheetNames.CLUSTER.value,
        ReferentialSheetNames.PEAK_PARAMS.value,
    ]

    columns_names_dict = {
        ReferentialSheetNames.PAYS.value: [c.value for c in CountryColumnsNames],
        ReferentialSheetNames.STUDY_SCENARIO.value: [c.value for c in StudyScenarioColumnsNames],
        ReferentialSheetNames.CLUSTER.value: [c.value for c in ClusterColumnsNames],
        ReferentialSheetNames.PEAK_PARAMS.value: [c.value for c in PeakParamsColumnsNames],
    }

    def __init__(self, path_file: Path, sheets_name: list[str] = target_sheet_names):
        self.path_file = path_file
        self.sheets_name = sheets_name

        self._parsefile()

    def _parsefile(self) -> None:
        if not self.path_file.exists():
            raise FileNotFoundError(f"Input file does not exist: {self.path_file}")

        # check sheets
        wb = load_workbook(filename=self.path_file)
        for sheet in wb.sheetnames:
            if sheet not in self.sheets_name:
                raise ValueError(f"Sheet '{sheet}' not found in MAIN_PARAMS.xlsx")

        # read sheets and put in a dict
        dict_of_df = {sheet: pd.read_excel(self.path_file, sheet_name=sheet) for sheet in self.sheets_name}

        # check every sheet/df must be with right columns
        for sheet, df in dict_of_df.items():
            if not set(df.columns.to_list()) == set(self.columns_names_dict[sheet]):
                raise ValueError(f"Columns names mismatch for sheet '{sheet}'")
