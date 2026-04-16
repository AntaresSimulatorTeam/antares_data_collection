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
import operator

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeAlias

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    InputThermalColumns,
    OutputHoursColumns,
)
from antares.data_collection.thermal.param_modulation.constants import (
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
)
from antares.data_collection.thermal.utils import (
    filter_input_based_on_study_scenarios,
    filter_thermal_input_file_based_on_commission_date,
    get_path_capacity_modulation_file,
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
    should_write_the_series: bool


@dataclass(frozen=True)
class SearchDirection:
    starting_point: float
    operator: Callable[[float, float], bool]


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
            InputThermalColumns.MARKET_NODE,
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

    def _build_cluster_group_repartition(
        self,
        df: pd.DataFrame,
        columns_to_use: list[str],
        group_index_to_internal_mapping: dict[int, InternalMapping],
        search_direction: SearchDirection,
        default_ts: pd.Series,
    ) -> ClusterGroupTsRepartition:
        result: ClusterGroupTsRepartition = {}

        useful_cols = columns_to_use + [InputThermalColumns.NET_MAX_GEN_CAP.value]
        df_groups = df[useful_cols].groupby(by=columns_to_use, dropna=False)
        for group, data in df_groups:
            zone = group[4]
            assert isinstance(zone, ZoneId)
            market_node = group[5]
            assert isinstance(market_node, str)
            cluster_id = group[3]
            assert isinstance(cluster_id, ClusterId)

            lowest_mean = search_direction.starting_point
            final_ts = pd.Series()

            # If no curve id is provided -> we SHOULD NOT write the final data
            # If a curve id is provided but does not exist in the index -> we SHOULD write the final data
            should_write_the_series = True
            curve_id_exists_but_not_present_in_index = False

            for group_index, internal_mapping in group_index_to_internal_mapping.items():
                value: str = group[group_index]  # type: ignore
                if pd.isna(value):
                    continue

                if value not in internal_mapping.index.get(zone, {}):
                    curve_id_exists_but_not_present_in_index = True
                    continue

                curve_ids = internal_mapping.index[zone][value]
                for curve_id in curve_ids:
                    ts = internal_mapping.data[curve_id]
                    ts_mean = ts.mean()
                    if search_direction.operator(lowest_mean, ts_mean):
                        final_ts = ts
                        lowest_mean = ts_mean

            # Use default value for empty rows
            if final_ts.empty:
                final_ts = default_ts
                if not curve_id_exists_but_not_present_in_index:
                    should_write_the_series = False

            # Fill the result
            weight = data[InputThermalColumns.NET_MAX_GEN_CAP].sum()
            ts_pair = TimeSeriesAndClusterPair(weight, final_ts, should_write_the_series)
            result.setdefault(market_node, {}).setdefault(cluster_id, []).append(ts_pair)

        return result

    def _build_must_run(self, df: pd.DataFrame, index_to_ts: IndexesToTimeSeries) -> ClusterGroupTsRepartition:
        must_run_cols = [
            InputThermalColumns.GRP_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            ANTARES_CLUSTER_NAME_COLUMN,
            InputThermalColumns.ZONE,
            InputThermalColumns.MARKET_NODE,
        ]

        mapping = {
            0: index_to_ts.group_must_run,
            1: index_to_ts.must_run,
            2: index_to_ts.inelastic,
        }

        direction = SearchDirection(starting_point=math.inf, operator=operator.ge)

        return self._build_cluster_group_repartition(df, must_run_cols, mapping, direction, DEFAULT_MUST_RUN_TS)

    def _build_capacity_modulation(
        self, df: pd.DataFrame, index_to_ts: IndexesToTimeSeries
    ) -> ClusterGroupTsRepartition:
        modulation_cols = [
            InputThermalColumns.GRP_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            ANTARES_CLUSTER_NAME_COLUMN,
            InputThermalColumns.ZONE,
            InputThermalColumns.MARKET_NODE,
        ]

        mapping = {
            0: index_to_ts.group_derating,
            1: index_to_ts.derating,
            2: index_to_ts.inelastic,
        }

        direction = SearchDirection(starting_point=-math.inf, operator=operator.le)
        default_ts = DEFAULT_CAPACITY_MODULATION_TS
        return self._build_cluster_group_repartition(df, modulation_cols, mapping, direction, default_ts)

    def _build_pegase_dataframe(self, data_repartition: ClusterGroupTsRepartition, year: int) -> pd.DataFrame:
        pegase_df_as_dict = {}
        # Sort values for output reproduction
        for market_node in sorted(data_repartition):
            for cluster in sorted(data_repartition[market_node]):
                ts_list = data_repartition[market_node][cluster]
                if len(ts_list) == 1:
                    if not ts_list[0].should_write_the_series:
                        # We don't want to write this TS, so we just skip it
                        continue
                    # We just need to write the TS as is
                    final_ts = ts_list[0].series
                else:
                    # We have to merge all TS in just one using the weights and the values of each TS
                    total_weight = sum(ts.weight for ts in ts_list)
                    final_ts = sum(ts.weight * ts.series for ts in ts_list) / total_weight  # type: ignore

                zone = self.main_params.get_antares_code(market_node)
                assert isinstance(zone, str)
                column_name = f"{zone}_{cluster}"
                pegase_df_as_dict[column_name] = final_ts

        df = pd.DataFrame.from_dict(pegase_df_as_dict)

        # Add the Hours columns
        cols_before_hours = list(df.columns)
        df[OutputHoursColumns.HOUR] = range(1, len(df) + 1)
        start_time = pd.to_datetime(f"01/01/{year} 00:00:00")
        df[OutputHoursColumns.DATE] = [str(start_time + pd.Timedelta(hours=i)) for i in range(len(df))]

        # We should put them as the first 2 columns for the user readability
        df = df[[OutputHoursColumns.DATE.value, OutputHoursColumns.HOUR.value] + cols_before_hours]

        # We want our dataframe to start on the 1st of July at midnight for PEGASE.
        # So we have to reindex it at the right index
        time_delta = pd.Timestamp(year=year, month=7, day=1, hour=0) - pd.Timestamp(year=year, month=1, day=1, hour=0)
        first_index = time_delta.days * 24
        new_index = list(range(first_index, len(df))) + list(range(1, first_index))
        return df.reindex(new_index)

    def _write_must_run_file(self, year: int, data_repartition: ClusterGroupTsRepartition) -> None:
        df = self._build_pegase_dataframe(data_repartition, year)
        file_path = self.output_folder / TECHNICAL_PARAMS_FOLDER / f"{MUST_RUN_OUTPUT_NAME}_{year - 1}-{year}.csv"
        write_csv_file(file_path, df)

    def _write_capacity_modulation_file(self, year: int, data_repartition: ClusterGroupTsRepartition) -> None:
        df = self._build_pegase_dataframe(data_repartition, year)
        file_path = get_path_capacity_modulation_file(year, self.output_folder)
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
