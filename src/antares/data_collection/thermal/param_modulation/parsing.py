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
import math

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
from antares.data_collection.thermal.param_modulation.constants import (
    CAPACITY_MODULATION_NAME,
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
    MUST_RUN_OUTPUT_NAME,
    SCENARIO_TO_ALWAYS_CONSIDER,
    TECHNICAL_PARAMS_FOLDER,
    InputGroupMustRunIndexColumns,
    InputIndexColumns,
    OutputHoursColumns,
)
from antares.data_collection.thermal.utils import (
    filter_input_based_on_study_scenarios,
    filter_thermal_input_file_based_on_commission_date,
    parse_input_file,
)
from antares.data_collection.utils import write_csv_file

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


@dataclass
class TimeSeriesAndClusterPair:
    weight: float
    series: pd.Series


ClusterGroupTsRepartition: TypeAlias = dict[ZoneId, dict[ClusterId, list[TimeSeriesAndClusterPair]]]


class ThermalParamModulationParser:
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

    def _build_must_run(self, df: pd.DataFrame, index_to_ts: IndexesToTimeSeries) -> ClusterGroupTsRepartition:
        result: ClusterGroupTsRepartition = {}

        must_run_cols = [
            InputThermalColumns.GRP_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            ANTARES_CLUSTER_NAME_COLUMN,
            InputThermalColumns.ZONE,
        ]
        useful_cols = must_run_cols + [InputThermalColumns.NET_MAX_GEN_CAP]
        must_run_groups = df[useful_cols].groupby(by=must_run_cols, dropna=False)
        for group, data in must_run_groups:
            zone = group[4]
            assert isinstance(zone, ZoneId)
            cluster_id = group[3]
            assert isinstance(cluster_id, ClusterId)

            # We want to select the Series with the lowest mean
            lowest_mean = math.inf
            final_ts = pd.Series()

            # Group Must Run column
            grp_must_run_value: str = group[0]  # type: ignore
            if not pd.isna(grp_must_run_value):
                if grp_must_run_value in index_to_ts.group_must_run.index.get(zone, {}):
                    curve_ids = index_to_ts.group_must_run.index[zone][grp_must_run_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.group_must_run.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean > ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Must Run column
            must_run_value: str = group[1]  # type: ignore
            if not pd.isna(must_run_value):
                if must_run_value in index_to_ts.must_run.index.get(zone, {}):
                    curve_ids = index_to_ts.must_run.index[zone][must_run_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.must_run.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean > ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Inelastic column
            inelastic_value: str = group[2]  # type: ignore
            if not pd.isna(inelastic_value):
                if inelastic_value in index_to_ts.inelastic.index.get(zone, {}):
                    curve_ids = index_to_ts.inelastic.index[zone][inelastic_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.inelastic.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean > ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Use default value for empty rows
            if final_ts.empty:
                final_ts = DEFAULT_MUST_RUN_TS

            # Fill the result
            weight = data[InputThermalColumns.NET_MAX_GEN_CAP].mean()
            ts_pair = TimeSeriesAndClusterPair(series=final_ts, weight=weight)
            result.setdefault(zone, {}).setdefault(cluster_id, []).append(ts_pair)

        return result

    def _build_capacity_modulation(
        self, df: pd.DataFrame, index_to_ts: IndexesToTimeSeries
    ) -> ClusterGroupTsRepartition:
        result: ClusterGroupTsRepartition = {}

        must_run_cols = [
            InputThermalColumns.GRP_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            ANTARES_CLUSTER_NAME_COLUMN,
            InputThermalColumns.ZONE,
        ]
        useful_cols = must_run_cols + [InputThermalColumns.NET_MAX_GEN_CAP]
        must_run_groups = df[useful_cols].groupby(by=must_run_cols, dropna=False)
        for group, data in must_run_groups:
            zone = group[4]
            assert isinstance(zone, ZoneId)
            cluster_id = group[3]
            assert isinstance(cluster_id, ClusterId)

            # We want to select the Series with the highest mean
            lowest_mean = math.inf
            final_ts = pd.Series()

            # Group Derating column
            grp_derating_value: str = group[0]  # type: ignore
            if not pd.isna(grp_derating_value):
                if grp_derating_value in index_to_ts.group_derating.index.get(zone, {}):
                    curve_ids = index_to_ts.group_derating.index[zone][grp_derating_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.group_derating.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean < ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Derating column
            derating_value: str = group[1]  # type: ignore
            if not pd.isna(derating_value):
                if derating_value in index_to_ts.derating.index.get(zone, {}):
                    curve_ids = index_to_ts.derating.index[zone][derating_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.derating.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean < ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Inelastic column
            inelastic_value: str = group[2]  # type: ignore
            if not pd.isna(inelastic_value):
                if inelastic_value in index_to_ts.inelastic.index.get(zone, {}):
                    curve_ids = index_to_ts.inelastic.index[zone][inelastic_value]
                    for curve_id in curve_ids:
                        ts = index_to_ts.inelastic.data[curve_id]
                        ts_mean = ts.mean()
                        if lowest_mean < ts_mean:
                            final_ts = ts
                            lowest_mean = ts_mean

            # Use default value for empty rows
            if final_ts.empty:
                final_ts = DEFAULT_CAPACITY_MODULATION_TS

            # Fill the result
            weight = data[InputThermalColumns.NET_MAX_GEN_CAP].mean()
            ts_pair = TimeSeriesAndClusterPair(series=final_ts, weight=weight)
            result.setdefault(zone, {}).setdefault(cluster_id, []).append(ts_pair)

        return result

    def _build_pegase_dataframe(self, data_repartition: ClusterGroupTsRepartition, year: int) -> pd.DataFrame:
        pegase_df_as_dict = {}
        # Sort values for output reproduction
        for zone in sorted(data_repartition):
            for cluster in sorted(data_repartition[zone]):
                ts_list = data_repartition[zone][cluster]
                if len(ts_list) == 1:
                    # We just need to write the TS as is
                    final_ts = ts_list[0].series
                else:
                    # We have to merge all TS in just one using the weights and the values of each TS
                    total_weight = sum(ts.weight for ts in ts_list)
                    final_ts = sum(ts.weight * ts.series for ts in ts_list) / total_weight  # type: ignore

                column_name = f"{zone}_{cluster}"
                pegase_df_as_dict[column_name] = final_ts

        df = pd.DataFrame.from_dict(pegase_df_as_dict)

        # Add the Hours columns
        df[OutputHoursColumns.HOUR] = range(1, len(df) + 1)
        start_time = pd.to_datetime(f"01/01/{year} 00:00:00")
        df[OutputHoursColumns.DATE] = [start_time + pd.Timedelta(hours=i) for i in range(len(df))]

        # We should put them as the first 2 columns for the user readability
        df = df[[OutputHoursColumns.DATE, OutputHoursColumns.HOUR] + list(df.columns)]

        # We want our dataframe to start on the 1st of July at midnight for PEGASE.
        # So we have to reindex it at the right index
        time_delta = pd.Timestamp(year=year, month=7, day=1, hour=0) - pd.Timestamp(year=year, month=1, day=1, hour=0)
        first_index = time_delta.days * 24
        new_index = list(range(first_index, len(df))) + list(range(1, first_index))
        return df.reindex(new_index)

    def _write_must_run_file(self, year: int, data_repartition: ClusterGroupTsRepartition) -> None:
        df = self._build_pegase_dataframe(data_repartition, year)
        file_path = self.output_folder / TECHNICAL_PARAMS_FOLDER / f"{MUST_RUN_OUTPUT_NAME}_{year}.csv"
        write_csv_file(file_path, df)

    def _write_capacity_modulation_file(self, year: int, data_repartition: ClusterGroupTsRepartition) -> None:
        df = self._build_pegase_dataframe(data_repartition, year)
        file_path = self.output_folder / TECHNICAL_PARAMS_FOLDER / f"{CAPACITY_MODULATION_NAME}_{year}.csv"
        write_csv_file(file_path, df)

    def build_param_modulation(self, thermal_df: pd.DataFrame) -> None:
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

            # Write the `Must Run` file
            must_run_cluster_group_ts_repartition = self._build_must_run(thermal_df_year, index_to_timeseries)
            self._write_must_run_file(year, must_run_cluster_group_ts_repartition)

            # Write the `Capacity Modulation` file
            capacity_modulation_repartition = self._build_capacity_modulation(thermal_df_year, index_to_timeseries)
            self._write_capacity_modulation_file(year, capacity_modulation_repartition)
