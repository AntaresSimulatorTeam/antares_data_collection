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
from pathlib import Path
from typing import TypeAlias

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    InputThermalColumns,
)
from antares.data_collection.thermal.technical_parameters.constants import (
    DERATING_INDEX_NAME,
    GROUP_DERATING_INDEX_NAME,
    GROUP_MUST_RUN_INDEX_NAME,
    GROUP_MUST_RUN_LABEL,
    INELASTIC_INDEX_NAME,
    MUST_RUN_INDEX_NAME,
    SCENARIO_TO_ALWAYS_CONSIDER,
    InputGroupMustRunIndexColumns,
    InputIndexColumns,
)
from antares.data_collection.thermal.utils import (
    filter_input_based_on_study_scenarios,
    filter_thermal_input_file_based_on_commission_date,
    parse_input_file,
)

ZoneId: TypeAlias = str
ClusterId: TypeAlias = str
CurveIds: TypeAlias = list[str]
IndexMapping: TypeAlias = dict[ZoneId, dict[ClusterId, CurveIds]]


@dataclass(frozen=True)
class AllIndexMappings:
    inelastic: IndexMapping
    group_must_run: IndexMapping
    must_run: IndexMapping
    derating: IndexMapping
    group_derating: IndexMapping


class ThermalSpecificParametersParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _parse_inelastic_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / INELASTIC_INDEX_NAME, list(InputIndexColumns))

    def _parse_derating_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / DERATING_INDEX_NAME, list(InputIndexColumns))

    def _parse_group_derating_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / GROUP_DERATING_INDEX_NAME, list(InputIndexColumns))

    def _parse_must_run_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / MUST_RUN_INDEX_NAME, list(InputIndexColumns))

    def _parse_group_must_run_index(self) -> pd.DataFrame:
        df = parse_input_file(self.input_folder / GROUP_MUST_RUN_INDEX_NAME, list(InputGroupMustRunIndexColumns))
        df = df[df[InputGroupMustRunIndexColumns.LABEL] == GROUP_MUST_RUN_LABEL]
        df = df.drop(columns=[InputGroupMustRunIndexColumns.LABEL])
        return df

    def _filter_index_files_with_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        scenario = self.main_params.get_scenario_type(year=year)
        acceptable_scenario_types = [SCENARIO_TO_ALWAYS_CONSIDER, f"{scenario}_{year}", f"All_years_{scenario}"]
        return df[df[InputIndexColumns.TARGET_YEAR].isin(acceptable_scenario_types)]

    def _build_index_mapping(self, df: pd.DataFrame, year: int) -> IndexMapping:
        columns_to_group = [InputIndexColumns.ZONE.value, InputIndexColumns.ID.value]
        return self._build_index_internal_mapping(df, year, columns_to_group, InputIndexColumns.CURVE_UID)

    def _build_group_must_run_index_mapping(self, df: pd.DataFrame, year: int) -> IndexMapping:
        columns_to_group = [InputGroupMustRunIndexColumns.ZONE.value, InputGroupMustRunIndexColumns.ID.value]
        return self._build_index_internal_mapping(df, year, columns_to_group, InputGroupMustRunIndexColumns.CURVE_UID)

    def _build_index_internal_mapping(
        self, df: pd.DataFrame, year: int, cols_to_group: list[str], curve_id_col: str
    ) -> IndexMapping:
        df = self._filter_index_files_with_year(df=df, year=year)
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: IndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, ClusterId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[curve_id_col])
        return mapping

    def _filter_thermal_input_file(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        df = filter_input_based_on_study_scenarios(df, self.main_params, [year])
        df = filter_thermal_input_file_based_on_commission_date(df, [year])
        useful_columns = [
            InputThermalColumns.ZONE,
            InputThermalColumns.NET_MAX_GEN_CAP,
            InputThermalColumns.GRP_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_MRUN_CURVE_ID,
            InputThermalColumns.GRP_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            ANTARES_CLUSTER_NAME_COLUMN,
            ANTARES_NODE_NAME_COLUMN,
        ]
        return df[useful_columns]

    def _build_mapping_dataclass(
        self,
        year: int,
        inelastic: pd.DataFrame,
        derating: pd.DataFrame,
        group_derating: pd.DataFrame,
        must_run: pd.DataFrame,
        group_must_run: pd.DataFrame,
    ) -> AllIndexMappings:
        inelastic_index_mapping = self._build_index_mapping(df=inelastic, year=year)
        derating_index_mapping = self._build_index_mapping(df=derating, year=year)
        group_derating_index_mapping = self._build_index_mapping(df=group_derating, year=year)
        must_run_index_mapping = self._build_index_mapping(df=must_run, year=year)
        group_must_run_index_mapping = self._build_group_must_run_index_mapping(group_must_run, year)

        return AllIndexMappings(
            inelastic=inelastic_index_mapping,
            group_must_run=group_must_run_index_mapping,
            must_run=must_run_index_mapping,
            derating=derating_index_mapping,
            group_derating=group_derating_index_mapping,
        )

    def _truc(self, df: pd.DataFrame, index_mappings: AllIndexMappings):
        """
        zones = list(df[InputThermalColumns.ZONE])
        net_max_gen_caps = list(df[InputThermalColumns.NET_MAX_GEN_CAP])
        group_must_runs = list(df[InputThermalColumns.GRP_MRUN_CURVE_ID])
        unit_must_runs = list(df[InputThermalColumns.GEN_UNT_MRUN_CURVE_ID])
        group_deratings = list(df[InputThermalColumns.GRP_D_CURVE_ID])
        unit_deratings = list(df[InputThermalColumns.GEN_UNT_D_CURVE_ID])
        inelastics = list(df[InputThermalColumns.GEN_UNT_INELASTIC_ID])
        antares_zones = list(df[ANTARES_NODE_NAME_COLUMN])
        antares_clusters = list(df[ANTARES_CLUSTER_NAME_COLUMN])

        for k in range(len(df)):
            if not pd.isna(group_must_runs[k]):
                print(group_must_runs[k])
        """

    # 1- Fair toutes es lignes
    # 2- Si aucune valeur est remplie, noter un truc
    # 3- Si une valeur est remplie aller prendre le mapping associé
    # Il faut construire un objet interne pour ça non ?
    # Du genre le IndexMapping mais avec ce dont on a vraiment besoin mais faudrait aussi le type ...

    def build_thermal_specific_parameters(self, thermal_df: pd.DataFrame) -> None:
        inelastic_index_df = self._parse_inelastic_index()
        group_must_run_index_df = self._parse_group_must_run_index()
        derating_index_df = self._parse_derating_index()
        group_derating_index_df = self._parse_group_derating_index()
        must_run_index_df = self._parse_must_run_index()

        for year in self.years:
            index_mappings = self._build_mapping_dataclass(
                year,
                inelastic_index_df,
                derating_index_df,
                group_derating_index_df,
                must_run_index_df,
                group_must_run_index_df,
            )

            thermal_df_year = self._filter_thermal_input_file(thermal_df, year)
            self._truc(thermal_df_year, index_mappings)
