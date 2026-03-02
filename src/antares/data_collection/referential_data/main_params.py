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
from dataclasses import dataclass

# structure Referential (MAIN_PARAMS.xlsx)
from enum import Enum, StrEnum
from pathlib import Path

import pandas as pd


# all workbook sheet names
class ReferentialSheetNames(StrEnum):
    PAYS = "PAYS"
    STUDY_SCENARIO = "STUDY_SCENARIO"
    LINKS = "LINKS"
    CLUSTER = "CLUSTER"
    PEAK_PARAMS = "PEAK_PARAMS"
    COMMON_DATA = "Common Data"


# sheet "PAYS"
class CountryColumnsNames(StrEnum):
    NOM_PAYS = "Nom_pays"
    CODE_PAYS = "code_pays"
    AREAS = "areas"
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# sheet "STUDY_SCENARIO"
class StudyScenarioColumnsNames(StrEnum):
    YEAR = "YEAR"
    STUDY_SCENARIO = "STUDY_SCENARIO"


# sheet "LINKS"
class LinksColumnsNames(Enum):
    MARKET_NODE = "market_node"
    CODE_ANTARES = "code_antares"


# sheet "CLUSTER"
class ClusterColumnsNames(StrEnum):
    TYPE = "TYPE"
    CLUSTER_PEMMDB = "CLUSTER_PEMMDB"
    CLUSTER_BP = "CLUSTER_BP"


# sheet "Common Data"
class CommonDataColumnsNames(StrEnum):
    CLUSTER_BP = "cluster_BP"
    FUEL = "Fuel"
    TYPE = "Type"


THERMAL_TYPE_NAME = "Thermal"


# "PEAK_PARAMS"
class PeakParamsColumnsNames(Enum):
    HOUR = "hour"
    PERIOD_HOUR = "period_hour"
    MONTH = "month"
    PERIOD_MONTH = "period_month"


@dataclass(frozen=True)
class ClusterParams:
    type: str
    fuel: str


@dataclass
class MainParams:
    """
    Container for validated referential parameters.

    This class encapsulates mappings extracted from MAIN_PARAMS.xlsx
    and exposes explicit getter methods to retrieve business values
    in a safe and controlled way.

    Internal dictionaries must not be accessed directly. All lookups
    should be performed through the provided getter methods to ensure
    consistent validation and explicit error handling.

    Attributes:
        _market_to_antares (dict[str, str]):
            Mapping from market node to Antares code.
        _year_to_scenario (dict[int, str]):
            Mapping from study year to scenario type.
        _cluster_antares (dict[str, ClusterParams]):
            Mapping from BP cluster to its attribute `fuel` and `type`
    """

    _market_to_antares: dict[str, str]
    _year_to_scenario: dict[int, str]
    _cluster_pemmdb_to_antares: dict[str, str]
    _cluster_antares: dict[str, ClusterParams]

    def get_antares_code(self, market_code: str) -> str:
        if market_code not in self._market_to_antares:
            raise ValueError(f"No antares code defined for market {market_code}")
        return self._market_to_antares[market_code]

    def get_antares_codes(self, market_codes: list[str]) -> list[str]:
        return [self.get_antares_code(c) for c in market_codes]

    def get_scenario_type(self, year: int) -> str:
        if year not in self._year_to_scenario:
            raise ValueError(f"No scenario defined for year {year}")
        return self._year_to_scenario[year]

    def get_scenario_types(self, years: list[int]) -> list[str]:
        return [self.get_scenario_type(y) for y in years]

    def get_cluster_bp(self, cluster_pemmdb: str) -> str:
        if cluster_pemmdb not in self._cluster_pemmdb_to_antares:
            raise ValueError(f"No cluster BP defined for cluster {cluster_pemmdb}")
        return self._cluster_pemmdb_to_antares[cluster_pemmdb]

    def get_clusters_bp(self, clusters_pemmdb: list[str]) -> list[str]:
        return [self.get_cluster_bp(c) for c in clusters_pemmdb]

    def _get_antares_cluster(self, antares_cluster: str) -> ClusterParams:
        if antares_cluster not in self._cluster_pemmdb_to_antares:
            raise ValueError(f"Cluster {antares_cluster} not found inside sheet {ReferentialSheetNames.COMMON_DATA}")
        return self._cluster_antares[antares_cluster]

    def get_antares_cluster_type(self, antares_cluster: str) -> str:
        return self._get_antares_cluster(antares_cluster).type

    def get_antares_cluster_fuel(self, antares_cluster: str) -> str:
        return self._get_antares_cluster(antares_cluster).fuel


def parse_main_params(file_path: Path) -> MainParams:
    """Parse and validate a MAIN_PARAMS.xlsx workbook.

    This function:
        1. Verifies that the Excel file exists.
        2. Reads required sheets using pandas.
        3. Validates required columns.
        4. Transforms each sheet into business mappings.
        5. Returns an immutable MainParams instance.

    The returned object provides explicit getter methods to safely
    access referential values.

    Args:
        file_path: Path to the MAIN_PARAMS.xlsx file.

    Returns:
        A validated MainParams object containing referential mappings.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If required sheets or columns are missing.
    """

    if not file_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    expected_sheets = [
        ReferentialSheetNames.PAYS,
        ReferentialSheetNames.STUDY_SCENARIO,
        ReferentialSheetNames.CLUSTER,
        ReferentialSheetNames.COMMON_DATA,
    ]
    excel_sheets = pd.read_excel(file_path, sheet_name=None)
    for sheet in expected_sheets:
        if sheet not in excel_sheets:
            raise ValueError(f"Worksheet named '{sheet}' not found")

    # Parse the `PAYS` sheet
    df = excel_sheets[ReferentialSheetNames.PAYS]
    actual_cols = set(df.columns)
    for column in [CountryColumnsNames.MARKET_NODE, CountryColumnsNames.CODE_ANTARES]:
        if column.value not in actual_cols:
            raise ValueError(f"Column '{column}' not found inside sheet '{ReferentialSheetNames.PAYS}'")

    countries_dict = dict(zip(df[CountryColumnsNames.MARKET_NODE], df[CountryColumnsNames.CODE_ANTARES]))

    # Parse the `STUDY_SCENARIO` sheet
    df = excel_sheets[ReferentialSheetNames.STUDY_SCENARIO]
    actual_cols = set(df.columns)
    for col in [StudyScenarioColumnsNames.YEAR, StudyScenarioColumnsNames.STUDY_SCENARIO]:
        if col.value not in actual_cols:
            raise ValueError(f"Column '{col}' not found inside sheet '{ReferentialSheetNames.STUDY_SCENARIO}'")

    scenario_dict = dict(zip(df[StudyScenarioColumnsNames.YEAR], df[StudyScenarioColumnsNames.STUDY_SCENARIO]))

    # Parse the `CLUSTER` sheet
    df = excel_sheets[ReferentialSheetNames.CLUSTER]
    actual_cols = set(df.columns)
    for cluster_col in [ClusterColumnsNames.CLUSTER_PEMMDB, ClusterColumnsNames.CLUSTER_BP, ClusterColumnsNames.TYPE]:
        if cluster_col.value not in actual_cols:
            raise ValueError(f"Column '{cluster_col}' not found inside sheet '{ReferentialSheetNames.CLUSTER}'")

    df = df[df[ClusterColumnsNames.TYPE] == THERMAL_TYPE_NAME]

    cluster_dict = dict(zip(df[ClusterColumnsNames.CLUSTER_PEMMDB.value], df[ClusterColumnsNames.CLUSTER_BP.value]))

    # Parse the `Common Data` sheet
    df = excel_sheets[ReferentialSheetNames.COMMON_DATA]
    actual_cols = set(df.columns)
    for common_col in [CommonDataColumnsNames.CLUSTER_BP, CommonDataColumnsNames.FUEL, CommonDataColumnsNames.TYPE]:
        if common_col.value not in actual_cols:
            raise ValueError(f"Column '{common_col}' not found inside sheet '{ReferentialSheetNames.COMMON_DATA}'")

    cluster_antares_dict = {
        row[CommonDataColumnsNames.CLUSTER_BP]: ClusterParams(
            type=row[CommonDataColumnsNames.TYPE], fuel=row[CommonDataColumnsNames.FUEL]
        )
        for _, row in df.iterrows()
    }

    # Return validated dataclass
    return MainParams(
        _market_to_antares=countries_dict,
        _year_to_scenario=scenario_dict,
        _cluster_pemmdb_to_antares=cluster_dict,
        _cluster_antares=cluster_antares_dict,
    )
