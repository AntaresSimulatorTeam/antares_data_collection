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
    TECHNOLOGY = "Technology thermal"


# sheet "Common Data"
class CommonDataColumnsNames(StrEnum):
    CLUSTER_BP = "cluster_BP"
    FUEL = "Fuel"


THERMAL_TYPE_NAME = "Thermal"


# "PEAK_PARAMS"
class PeakParamsColumnsNames(Enum):
    HOUR = "hour"
    PERIOD_HOUR = "period_hour"
    MONTH = "month"
    PERIOD_MONTH = "period_month"


@dataclass(frozen=True)
class ClusterParams:
    technology: str
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

    def get_antares_code(self, market_code: str) -> str | None:
        value = self._market_to_antares.get(market_code)
        if pd.isna(value):
            # The value is either missing or the line does not even exist. We should log the information but not crash.
            print(f"Market node '{market_code}' was not found inside `MainParams`")
            return None
        return value

    def get_antares_codes(self, market_codes: list[str]) -> list[str | None]:
        return [self.get_antares_code(c) for c in market_codes]

    def get_scenario_type(self, year: int) -> str:
        if year not in self._year_to_scenario:
            raise ValueError(f"No scenario defined for year {year}")
        return self._year_to_scenario[year]

    def get_scenario_types(self, years: list[int]) -> set[str]:
        return {self.get_scenario_type(y) for y in years}

    def get_cluster_bp(self, cluster_pemmdb: str) -> str:
        if cluster_pemmdb not in self._cluster_pemmdb_to_antares:
            raise ValueError(f"No cluster BP defined for cluster {cluster_pemmdb}")
        return self._cluster_pemmdb_to_antares[cluster_pemmdb]

    def get_clusters_bp(self, clusters_pemmdb: list[str]) -> list[str]:
        return [self.get_cluster_bp(c) for c in clusters_pemmdb]

    def get_antares_cluster_technology_and_fuel(self, antares_cluster: str) -> ClusterParams:
        if antares_cluster not in self._cluster_antares:
            raise ValueError(f"Cluster {antares_cluster} not found inside sheet {ReferentialSheetNames.COMMON_DATA}")
        return self._cluster_antares[antares_cluster]


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
    for cluster_col in [
        ClusterColumnsNames.CLUSTER_PEMMDB,
        ClusterColumnsNames.CLUSTER_BP,
        ClusterColumnsNames.TYPE,
        ClusterColumnsNames.TECHNOLOGY,
    ]:
        if cluster_col.value not in actual_cols:
            raise ValueError(f"Column '{cluster_col}' not found inside sheet '{ReferentialSheetNames.CLUSTER}'")

    df = df[df[ClusterColumnsNames.TYPE] == THERMAL_TYPE_NAME]

    pemmdb_to_antares_mapping = {}
    intermediate_dict = {}  # Used to get the Technology attribute for the upcoming `ClusterParams` class
    for _, row in df.iterrows():
        pemmdb_to_antares_mapping[row[ClusterColumnsNames.CLUSTER_PEMMDB]] = row[ClusterColumnsNames.CLUSTER_BP]
        intermediate_dict[row[ClusterColumnsNames.CLUSTER_BP]] = row[ClusterColumnsNames.TECHNOLOGY]

    # Parse the `Common Data` sheet
    df = excel_sheets[ReferentialSheetNames.COMMON_DATA]
    actual_cols = set(df.columns)
    for common_col in [CommonDataColumnsNames.CLUSTER_BP, CommonDataColumnsNames.FUEL]:
        if common_col.value not in actual_cols:
            raise ValueError(f"Column '{common_col}' not found inside sheet '{ReferentialSheetNames.COMMON_DATA}'")

    cluster_antares_dict = {}
    for _, row in df.iterrows():
        bp_name = row[CommonDataColumnsNames.CLUSTER_BP]
        fuel = row[CommonDataColumnsNames.FUEL]
        cluster_antares_dict[bp_name] = ClusterParams(technology=intermediate_dict[bp_name], fuel=fuel)

    # Return validated dataclass
    return MainParams(
        _market_to_antares=countries_dict,
        _year_to_scenario=scenario_dict,
        _cluster_pemmdb_to_antares=pemmdb_to_antares_mapping,
        _cluster_antares=cluster_antares_dict,
    )
