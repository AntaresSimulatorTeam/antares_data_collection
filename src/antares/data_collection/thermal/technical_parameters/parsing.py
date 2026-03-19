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
    GROUP_MUST_RUN_INDEX_NAME,
    GROUP_MUST_RUN_LABEL,
    INELASTIC_INDEX_NAME,
    SCENARIO_TO_ALWAYS_CONSIDER,
    InputGroupMustRunIndexColumns,
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

    def _parse_group_must_run_index(self) -> pd.DataFrame:
        df =  parse_input_file(self.input_folder / GROUP_MUST_RUN_INDEX_NAME, list(InputGroupMustRunIndexColumns))
        df = df[df[InputGroupMustRunIndexColumns.LABEL] == GROUP_MUST_RUN_LABEL]
        df = df.drop(columns=[InputGroupMustRunIndexColumns.LABEL])
        return df

    def _filter_index_files_with_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        scenario = self.main_params.get_scenario_type(year=year)
        acceptable_scenario_types = [SCENARIO_TO_ALWAYS_CONSIDER, f"{scenario}_{year}", f"All_years_{scenario}"]
        return df[df[InputInelasticIndexColumns.TARGET_YEAR].isin(acceptable_scenario_types)]

    def _build_inelastic_index_mapping(self, df: pd.DataFrame, year: int) -> InelasticIndexMapping:
        columns_to_group = [InputInelasticIndexColumns.ZONE.value, InputInelasticIndexColumns.ID.value]
        return self._build_index_mapping(df, year, columns_to_group, InputInelasticIndexColumns.CURVE_UID)

    def _build_group_must_run_index_mapping(self, df: pd.DataFrame, year: int) -> InelasticIndexMapping:
        columns_to_group = [InputGroupMustRunIndexColumns.ZONE.value, InputGroupMustRunIndexColumns.ID.value]
        return self._build_index_mapping(df, year, columns_to_group, InputGroupMustRunIndexColumns.CURVE_UID)

    def _build_index_mapping(self, df: pd.DataFrame, year: int, cols_to_group: list[str], curve_id_col: str) -> InelasticIndexMapping:
        df = self._filter_index_files_with_year(df=df, year=year)
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: InelasticIndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, ClusterId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[curve_id_col])
        return mapping

    def build_thermal_specific_parameters(self, df: pd.DataFrame) -> None:
        inelastic_index_df = self._parse_inelastic_index()
        group_must_run_index_df = self._parse_group_must_run_index()
        for year in self.years:
            inelastic_index_mapping = self._build_inelastic_index_mapping(df=inelastic_index_df, year=year)
            group_must_run_index_mapping = self._build_group_must_run_index_mapping(df=group_must_run_index_df, year=year)

