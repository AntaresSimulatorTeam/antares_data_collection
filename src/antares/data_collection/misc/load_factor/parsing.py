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

from antares.data_collection.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    SCENARIO_TO_ALWAYS_CONSIDER,
)
from antares.data_collection.misc.constants import InputMiscColumns
from antares.data_collection.misc.load_factor.constants import (
    LOAD_FACTOR_FILE_INDEX_NAME,
    LOAD_FACTOR_FILE_TS_NAME,
    InputLoadFactorIndexColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_index_files_with_scenario_year,
    filter_out_based_on_year,
    parse_input_file,
)

# build structured index
ZoneId: TypeAlias = str
CurveId: TypeAlias = str

# one curve id can be associated to multiple curve uid
CurveUIDIds: TypeAlias = list[str]

IndexTsMapping: TypeAlias = dict[ZoneId, dict[CurveId, CurveUIDIds]]

@dataclass(frozen=True)
class InternalIndexTsMapping:
    index: IndexTsMapping
    data: pd.DataFrame

# need weight indexed to compute weighted average with time series then
AntaresCodeId: TypeAlias = str
ClusterId: TypeAlias = str
WeightValue: TypeAlias = float
WeightedAverage: TypeAlias = float


IndexClusterWeight: TypeAlias = dict[AntaresCodeId, dict[ClusterId, dict[CurveId, WeightValue]]]
IndexTimeSeriesWeightedAverage: TypeAlias = dict[AntaresCodeId, dict[ClusterId, WeightedAverage]]


class LoadFactorParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        main_params: MainParams,
        years: list[int],
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years


    def _read_input_file(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(LOAD_FACTOR_FILE_INDEX_NAME), list(InputLoadFactorIndexColumns))

    def _build_index_mapping_year(
        self, df: pd.DataFrame, year: int
    ) -> IndexTsMapping:
        columns_to_group = [InputLoadFactorIndexColumns.ZONE, InputLoadFactorIndexColumns.ID]
        df = filter_index_files_with_scenario_year(
            self.main_params,
            df=df,
            year=year,
            filter_scenario_value=SCENARIO_TO_ALWAYS_CONSIDER,
            target_year_col=InputLoadFactorIndexColumns.TARGET_YEAR.value,
        )
        groups = df.groupby(by=columns_to_group, as_index=False)
        mapping: IndexTsMapping = {}
        for (area, curve), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(curve, CurveId)
            mapping.setdefault(area, {})[curve] = list(grouped_df[InputLoadFactorIndexColumns.CURVE_UID])
        return mapping

    def _build_index_weight_year(self, df: pd.DataFrame, year: int) -> IndexClusterWeight:
        # filter data from MiscParser
        df = filter_out_based_on_year(
            df,
            year,
            InputMiscColumns.COMMISSIONING_DATE,
            InputMiscColumns.DECOMMISSIONING_DATE_EXPECTED,
        )

        # group by zone, cluster, curve
        name_col_curve_id = InputMiscColumns.CURVE_ID.value
        name_col_capacity = InputMiscColumns.NET_MAX_GEN_CAP.value

        group_cols = [ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN]
        subgroup_cols = group_cols + [name_col_curve_id]

        # 1. Aggregate capacity by (zone, cluster, curve)
        df_weights = df.groupby(subgroup_cols, as_index=False)[name_col_capacity].sum()

        # 2. Compute total capacity per cluster
        df_cluster_total = (
            df.groupby(group_cols, as_index=False)[[name_col_capacity]]
            .sum()
            .rename(columns={name_col_capacity: "total_capacity"})
        )

        # 3. Merge and compute weights
        df_weights = df_weights.merge(df_cluster_total, on=group_cols)
        df_weights[name_col_capacity] = (
                df_weights[name_col_capacity] / df_weights["total_capacity"]
        )

        # 3. Structure into a dictionary
        dict_of_weight: IndexClusterWeight = {}
        for _, row in df_weights.iterrows():
            area = row[ANTARES_NODE_NAME_COLUMN]
            cluster = row[ANTARES_CLUSTER_NAME_COLUMN]
            curve = row[name_col_curve_id]
            weight = row[name_col_capacity]

            dict_of_weight.setdefault(area, {}).setdefault(cluster, {})[curve] = weight

        return dict_of_weight

    def _build_index_ts_weighted_average_year(self, IndexMapping: InternalIndexTsMapping, IndexWeightCluster: IndexClusterWeight) -> IndexTimeSeriesWeightedAverage:
        result: IndexTimeSeriesWeightedAverage = {}

        # for each zone/cluster/curve from MiscParser indexed extract time series
        for antares_id, clusters in IndexWeightCluster.items():
            for cluster_id, curves in clusters.items():
                for curve_id, weight in curves.items():
                    uid_name = IndexMapping.index.get(antares_id, {}).get(curve_id)

                    if uid_name is None:
                        series_df = pd.DataFrame({"col": [1] * 8760})
                    else:
                        series_df = IndexMapping.data[uid_name]

                    # compute mean of several time series if necessary (for one curve id can be associated with several curve uid)
                    if len(series_df.columns) > 1:
                        series = series_df.mean(axis=1)
                    else:
                        series = series_df.iloc[:, 0]

                    (
                        result.setdefault(antares_id, {})
                        .setdefault(cluster_id, {})
                        .setdefault(curve_id, [])
                        .append(series)
                    )

        return result


    def build_load_factor(self, df_misc_filtered: pd.DataFrame) -> None:
        # parsing index file
        df_index = self._read_input_file()

        # parsing ts file
        df_ts = pd.read_csv(self.input_folder / LOAD_FACTOR_FILE_TS_NAME)

        # treatments for every year
        index_of_df_pegase: dict[int, pd.DataFrame] = {}
        for year in self.years:
            # group index filtered by year + time series (raw data)
            index_mapping_year = self._build_index_mapping_year(df_index, year)

            # structure index and time series dataclass
            index_ts_dataclass_year = InternalIndexTsMapping(index=index_mapping_year, data=df_ts)

            # buil dictionary with weight by zone/cluster/curve from MiscParser data frame
            index_cluster_weight = self._build_index_weight_year(df_misc_filtered, year)

            # build dictionary with zone/cluster who contains weighted average time series
            index_ts_weighted_average = self._build_index_ts_weighted_average_year(index_ts_dataclass_year, index_cluster_weight)








