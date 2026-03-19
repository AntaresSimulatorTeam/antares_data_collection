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
    DEFAULT_CAPACITY_MODULATION_TS,
    DEFAULT_MUST_RUN_TS,
    DERATING_INDEX_NAME,
    DERATING_NAME,
    GROUP_DERATING_INDEX_NAME,
    GROUP_DERATING_NAME,
    GROUP_MUST_RUN_INDEX_NAME,
    GROUP_MUST_RUN_LABEL,
    GROUP_MUST_RUN_NAME,
    INELASTIC_INDEX_NAME,
    INELASTIC_NAME,
    MUST_RUN_INDEX_NAME,
    MUST_RUN_NAME,
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
class InternalMapping:
    index: IndexMapping
    data: pd.DataFrame


@dataclass(frozen=True)
class IndexesToTimeSeries:
    inelastic: InternalMapping
    group_must_run: InternalMapping
    must_run: InternalMapping
    derating: InternalMapping
    group_derating: InternalMapping


Curves: TypeAlias = dict[int, tuple[pd.Series, float]]


@dataclass
class OutputData:
    must_run: Curves
    derating: Curves


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

    def _build_index_to_timeseries_object(
        self,
        year: int,
        *,
        inelastic_index: pd.DataFrame,
        derating_index: pd.DataFrame,
        group_derating_index: pd.DataFrame,
        must_run_index: pd.DataFrame,
        group_must_run_index: pd.DataFrame,
        inelastic: pd.DataFrame,
        derating: pd.DataFrame,
        group_derating: pd.DataFrame,
        must_run: pd.DataFrame,
        group_must_run: pd.DataFrame,
    ) -> IndexesToTimeSeries:
        inelastic_index_mapping = self._build_index_mapping(df=inelastic_index, year=year)
        derating_index_mapping = self._build_index_mapping(df=derating_index, year=year)
        group_derating_index_mapping = self._build_index_mapping(df=group_derating_index, year=year)
        must_run_index_mapping = self._build_index_mapping(df=must_run_index, year=year)
        group_must_run_index_mapping = self._build_group_must_run_index_mapping(group_must_run_index, year)

        return IndexesToTimeSeries(
            inelastic=InternalMapping(index=inelastic_index_mapping, data=inelastic),
            group_must_run=InternalMapping(index=group_must_run_index_mapping, data=group_must_run),
            must_run=InternalMapping(index=must_run_index_mapping, data=must_run),
            derating=InternalMapping(index=derating_index_mapping, data=derating),
            group_derating=InternalMapping(index=group_derating_index_mapping, data=group_derating),
        )

    def _builds_the_output_data(self, df: pd.DataFrame, index_to_ts: IndexesToTimeSeries) -> OutputData:
        zones = list(df[InputThermalColumns.ZONE])
        group_must_runs = list(df[InputThermalColumns.GRP_MRUN_CURVE_ID])
        unit_must_runs = list(df[InputThermalColumns.GEN_UNT_MRUN_CURVE_ID])
        group_deratings = list(df[InputThermalColumns.GRP_D_CURVE_ID])
        unit_deratings = list(df[InputThermalColumns.GEN_UNT_D_CURVE_ID])
        inelastics = list(df[InputThermalColumns.GEN_UNT_INELASTIC_ID])

        # Builds output data object
        output_data = OutputData(must_run={}, derating={})

        for k in range(len(df)):
            zone = zones[k]

            # First we handle the `Must Run` part.
            # We want to select the Series with the lowest mean

            # Group Must Run
            grp_must_run_value = group_must_runs[k]
            if not pd.isna(grp_must_run_value):
                if grp_must_run_value not in index_to_ts.group_must_run.index[zone]:
                    continue
                curve_ids = index_to_ts.group_must_run.index[zone][grp_must_run_value]
                for curve_id in curve_ids:
                    ts = index_to_ts.group_must_run.data[curve_id]
                    ts_mean = ts.mean()
                    if k not in output_data.must_run or output_data.must_run[k][1] > ts_mean:
                        output_data.must_run[k] = (ts, ts_mean)

            # Must Run
            must_run_value = unit_must_runs[k]
            if not pd.isna(must_run_value):
                if must_run_value not in index_to_ts.must_run.index[zone]:
                    continue
                curve_ids = index_to_ts.must_run.index[zone][must_run_value]
                for curve_id in curve_ids:
                    ts = index_to_ts.must_run.data[curve_id]
                    ts_mean = ts.mean()
                    if k not in output_data.must_run or output_data.must_run[k][1] > ts_mean:
                        output_data.must_run[k] = (ts, ts_mean)

            # Then we handle the `Derating` part.
            # We want to select the Series with the highest mean

            # Group Derating
            group_derating_value = group_deratings[k]
            if not pd.isna(group_derating_value):
                if group_derating_value not in index_to_ts.group_derating.index[zone]:
                    continue
                curve_ids = index_to_ts.group_derating.index[zone][group_derating_value]
                for curve_id in curve_ids:
                    ts = index_to_ts.group_derating.data[curve_id]
                    ts_mean = ts.mean()
                    if k not in output_data.derating or output_data.derating[k][1] < ts_mean:
                        output_data.derating[k] = (ts, ts_mean)

            # Derating
            derating_value = unit_deratings[k]
            if not pd.isna(derating_value):
                if derating_value not in index_to_ts.derating.index[zone]:
                    continue
                curve_ids = index_to_ts.derating.index[zone][derating_value]
                for curve_id in curve_ids:
                    ts = index_to_ts.derating.data[curve_id]
                    ts_mean = ts.mean()
                    if k not in output_data.derating or output_data.derating[k][1] < ts_mean:
                        output_data.derating[k] = (ts, ts_mean)

            # Inelastic
            inelastic_value = inelastics[k]
            if not pd.isna(inelastic_value):
                if inelastic_value not in index_to_ts.inelastic.index[zone]:
                    continue
                curve_ids = index_to_ts.inelastic.index[zone][inelastic_value]
                for curve_id in curve_ids:
                    ts = index_to_ts.inelastic.data[curve_id]
                    ts_mean = ts.mean()
                    # Inelastic should be considered for both `derating` and `must_run`
                    if k not in output_data.derating or output_data.derating[k][1] < ts_mean:
                        output_data.derating[k] = (ts, ts_mean)
                    if k not in output_data.must_run or output_data.must_run[k][1] > ts_mean:
                        output_data.must_run[k] = (ts, ts_mean)

            # If no value exists, fill the object with the default dataframes.
            if k not in output_data.derating:
                output_data.derating[k] = (DEFAULT_CAPACITY_MODULATION_TS, 1)
            if k not in output_data.must_run:
                output_data.must_run[k] = (DEFAULT_MUST_RUN_TS, 0)

        return output_data

    def build_thermal_specific_parameters(self, thermal_df: pd.DataFrame) -> None:
        # Parse Index files
        inelastic_index_df = self._parse_inelastic_index()
        group_must_run_index_df = self._parse_group_must_run_index()
        derating_index_df = self._parse_derating_index()
        group_derating_index_df = self._parse_group_derating_index()
        must_run_index_df = self._parse_must_run_index()

        # Parse data files
        inelastic_df = pd.read_csv(self.input_folder / INELASTIC_NAME)
        must_run_df = pd.read_csv(self.input_folder / MUST_RUN_NAME)
        group_must_run_df = pd.read_csv(self.input_folder / GROUP_MUST_RUN_NAME)
        derating_df = pd.read_csv(self.input_folder / DERATING_NAME)
        group_derating_df = pd.read_csv(self.input_folder / GROUP_DERATING_NAME)

        for year in self.years:
            # Builds an object with the whole data regrouped
            index_to_timeseries = self._build_index_to_timeseries_object(
                year,
                inelastic_index=inelastic_index_df,
                derating_index=derating_index_df,
                group_derating_index=group_derating_index_df,
                must_run_index=must_run_index_df,
                group_must_run_index=group_must_run_index_df,
                inelastic=inelastic_df,
                derating=derating_df,
                group_derating=group_derating_df,
                must_run=must_run_df,
                group_must_run=group_must_run_df,
            )

            thermal_df_year = self._filter_thermal_input_file(thermal_df, year)
            output_data = self._builds_the_output_data(thermal_df_year, index_to_timeseries)
            print(output_data)
