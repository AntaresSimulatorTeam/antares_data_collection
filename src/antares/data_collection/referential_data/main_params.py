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
from enum import Enum
from pathlib import Path

import pandas as pd


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
        _cluster_pemmdb_to_antares (dict[str, str]):
            Mapping from PEMMDB cluster to Antares cluster BP.
    """

    _market_to_antares: dict[str, str]
    _year_to_scenario: dict[int, str]
    _cluster_pemmdb_to_antares: dict[str, str]

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


def parse_main_params(file_path: Path) -> MainParams:
    """Parse and validate a MAIN_PARAMS.xlsx workbook.

    This function:
        1. Verifies that the Excel file exists.
        2. Reads required sheets using pandas.
        3. Validates required columns via `usecols`.
        4. Transforms each sheet into business mappings.
        5. Returns an immutable MainParams instance.

    The returned object provides explicit getter methods to safely
    access referential values.

    Expected sheets:
        - PAYS
        - STUDY_SCENARIO
        - CLUSTER

    Expected mappings:
        - market_node -> code_antares
        - year -> study_scenario
        - cluster_pemmdb -> cluster_bp

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

    columns_names_dict = {
        ReferentialSheetNames.PAYS.value: [c.value for c in CountryColumnsNames],
        ReferentialSheetNames.STUDY_SCENARIO.value: [c.value for c in StudyScenarioColumnsNames],
        ReferentialSheetNames.CLUSTER.value: [c.value for c in ClusterColumnsNames],
    }

    excel_sheets = pd.read_excel(file_path, sheet_name=None)
    for col in columns_names_dict:
        if col not in excel_sheets:
            raise ValueError(f"Worksheet named '{col}' not found")

    # parse sheets + check on sheets and columns by pandas
    df_countries = pd.read_excel(
        file_path,
        sheet_name=ReferentialSheetNames.PAYS.value,
        usecols=columns_names_dict[ReferentialSheetNames.PAYS.value],
    )

    # strict typed conversion
    countries_dict: dict[str, str] = dict(
        zip(
            df_countries[CountryColumnsNames.MARKET_NODE.value],
            df_countries[CountryColumnsNames.CODE_ANTARES.value],
        )
    )

    df_scenario = pd.read_excel(
        file_path,
        sheet_name=ReferentialSheetNames.STUDY_SCENARIO.value,
        usecols=columns_names_dict[ReferentialSheetNames.STUDY_SCENARIO.value],
    )

    # strict typed conversion
    scenario_dict: dict[int, str] = dict(
        zip(
            df_scenario[StudyScenarioColumnsNames.YEAR.value],
            df_scenario[StudyScenarioColumnsNames.STUDY_SCENARIO.value],
        )
    )

    df_cluster = pd.read_excel(
        file_path,
        sheet_name=ReferentialSheetNames.CLUSTER.value,
        usecols=columns_names_dict[ReferentialSheetNames.CLUSTER.value],
    )

    # strict typed conversion
    cluster_dict: dict[str, str] = dict(
        zip(
            df_cluster[ClusterColumnsNames.CLUSTER_PEMMDB.value],
            df_cluster[ClusterColumnsNames.CLUSTER_BP.value],
        )
    )

    # Return validated dataclass
    return MainParams(
        _market_to_antares=countries_dict, _year_to_scenario=scenario_dict, _cluster_pemmdb_to_antares=cluster_dict
    )
