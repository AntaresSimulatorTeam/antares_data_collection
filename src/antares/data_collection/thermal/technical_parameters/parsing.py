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
from typing import TypeAlias

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.technical_parameters.constants import (
    INELASTIC_INDEX_NAME,
    SCENARIO_TO_ALWAYS_CONSIDER,
    InputInelasticIndexColumns,
)
from antares.data_collection.thermal.utils import parse_input_file

ZoneId: TypeAlias = str
ClusterId: TypeAlias = str
CurveIds: TypeAlias = list[str]
InelasticIndexMapping: TypeAlias = dict[ZoneId, dict[ClusterId, CurveIds]]


class ThermalSpecificParametersParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _parse_inelastic_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / INELASTIC_INDEX_NAME, list(InputInelasticIndexColumns))

    def _filter_index_files_with_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        scenario = self.main_params.get_scenario_type(year=year)
        acceptable_scenario_types = [SCENARIO_TO_ALWAYS_CONSIDER, f"{scenario}_{year}", f"All_years_{scenario}"]
        return df[df[InputInelasticIndexColumns.TARGET_YEAR].isin(acceptable_scenario_types)]

    def _build_inelastic_index_mapping(self, df: pd.DataFrame, year: int) -> InelasticIndexMapping:
        df = self._filter_index_files_with_year(df=df, year=year)
        useful_cols = set(InputInelasticIndexColumns)
        useful_cols.remove(InputInelasticIndexColumns.TARGET_YEAR)
        groups = df.groupby(by=[InputInelasticIndexColumns.ZONE, InputInelasticIndexColumns.ID], as_index=False)
        mapping: InelasticIndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, ClusterId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[InputInelasticIndexColumns.CURVE_UID])
        return mapping

    def build_thermal_specific_parameters(self, df: pd.DataFrame) -> None:
        inelastic_index_df = self._parse_inelastic_index()
        for year in self.years:
            inelastic_index_mapping = self._build_inelastic_index_mapping(df=inelastic_index_df, year=year)
            print(inelastic_index_mapping)
