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
    OUTPUT_DATE_INT_REFERENCE,
    SCENARIO_TO_ALWAYS_CONSIDER,
)
from antares.data_collection.misc.constants import InputMiscColumns
from antares.data_collection.misc.load_factor.constants import (
    EXPORT_DATE_COLUMN,
    LOAD_FACTOR_FILE_INDEX_NAME,
    LOAD_FACTOR_FILE_TS_NAME,
    MISC_LOAD_FACTOR_FOLDER,
    InputLoadFactorIndexColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_index_files_with_scenario_year,
    filter_out_based_on_year,
    insert_str_date_time_reindex,
    parse_input_file,
    write_csv_file,
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
PemmdbPlantTypeId: TypeAlias = str

ClusterId: TypeAlias = str
WeightValue: TypeAlias = float
WeightedAverageTS: TypeAlias = pd.Series

# aggregated data must be indexed by zone/antares_code/cluster pemmdb/cluster/curve to match with index/ts data indexed only with zone
IndexClusterWeight: TypeAlias = dict[
    ZoneId, dict[AntaresCodeId, dict[tuple[PemmdbPlantTypeId, ClusterId], dict[CurveId, WeightValue]]]
]
IndexTimeSeriesWeightedAverage: TypeAlias = dict[
    AntaresCodeId, dict[tuple[PemmdbPlantTypeId, ClusterId], WeightedAverageTS]
]


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
        return parse_input_file(
            self.input_folder.joinpath(LOAD_FACTOR_FILE_INDEX_NAME), list(InputLoadFactorIndexColumns)
        )

    def _build_index_mapping_year(self, df: pd.DataFrame, year: int) -> IndexTsMapping:
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
        df = filter_out_based_on_year(
            df,
            year,
            InputMiscColumns.COMMISSIONING_DATE,
            InputMiscColumns.DECOMMISSIONING_DATE_EXPECTED,
        )

        # group by zone, cluster, curve
        name_col_curve_id = InputMiscColumns.CURVE_ID.value
        name_col_capacity = InputMiscColumns.NET_MAX_GEN_CAP.value

        group_cols = [
            InputMiscColumns.ZONE,
            ANTARES_NODE_NAME_COLUMN,
            InputMiscColumns.PEMMDB_PLANT_TYPE,
            ANTARES_CLUSTER_NAME_COLUMN,
        ]
        subgroup_cols = group_cols + [name_col_curve_id]

        # Aggregate capacity by (zone, antares code, cluster, curve)
        df_weights = df.groupby(subgroup_cols, as_index=False)[name_col_capacity].sum()

        # Compute total capacity per cluster
        df_cluster_total = (
            df.groupby(group_cols, as_index=False)[[name_col_capacity]]
            .sum()
            .rename(columns={name_col_capacity: "total_capacity"})
        )

        # Merge and compute weights
        df_weights = df_weights.merge(df_cluster_total, on=group_cols)
        df_weights[name_col_capacity] = df_weights[name_col_capacity] / df_weights["total_capacity"]

        # Structure into a dictionary
        dict_of_weight: IndexClusterWeight = {}
        for _, row in df_weights.iterrows():
            zone = row[InputMiscColumns.ZONE]
            area = row[ANTARES_NODE_NAME_COLUMN]
            pemmdb_cluster = row[InputMiscColumns.PEMMDB_PLANT_TYPE]
            cluster = row[ANTARES_CLUSTER_NAME_COLUMN]
            curve = row[name_col_curve_id]
            weight = row[name_col_capacity]

            dict_of_weight.setdefault(zone, {}).setdefault(area, {}).setdefault((pemmdb_cluster, cluster), {})[
                curve
            ] = weight

        return dict_of_weight

    def _build_index_ts_weighted_average_year(
        self, index_mapping: InternalIndexTsMapping, index_weight_cluster: IndexClusterWeight
    ) -> IndexTimeSeriesWeightedAverage:
        result: IndexTimeSeriesWeightedAverage = {}

        for zone_id, antares_data in index_weight_cluster.items():
            for antares_id, tuple_clusters in antares_data.items():
                for cluster_id, curves in tuple_clusters.items():
                    # Init for aggregated series by cluster
                    cluster_ts = pd.Series(0.0, index=range(8760))

                    for curve_id, weight in curves.items():
                        # get name(s) of ts uid(s)
                        uids = index_mapping.index.get(zone_id, {}).get(curve_id, [])

                        if not uids:
                            # default series if no mapping
                            series = pd.Series(1.0, index=range(8760))
                        else:
                            series_df = index_mapping.data[uids]
                            # apply mean if multi uids for one curve_id
                            series = series_df.mean(axis=1)

                        # sum of weighted average of several time series
                        cluster_ts += series * weight

                    result.setdefault(antares_id, {})[cluster_id] = cluster_ts

        return result

    def _build_pegase_dataframe(
        self, data_repartition: IndexTimeSeriesWeightedAverage
    ) -> dict[tuple[str, str], pd.DataFrame]:
        # rebuild group by cluster
        clusters_data: dict[tuple[PemmdbPlantTypeId, ClusterId], dict[AntaresCodeId, pd.Series]] = {}

        for antares_id, clusters in data_repartition.items():
            for cluster_id, ts in clusters.items():
                # rebuild index to structure data by cluster
                if cluster_id not in clusters_data:
                    clusters_data[cluster_id] = {}
                clusters_data[cluster_id][antares_id] = ts

        # return for each cluster a dataframe formatted with all the time series
        result: dict[tuple[str, str], pd.DataFrame] = {}
        for cluster_id, columns in clusters_data.items():
            df_cluster = pd.DataFrame(columns)

            # add formatted columns + reindex dataframe
            df_formatted = insert_str_date_time_reindex(df_cluster, OUTPUT_DATE_INT_REFERENCE, EXPORT_DATE_COLUMN)

            result[cluster_id] = df_formatted

        return result

    def _export_load_factor(
        self, index_of_df_year: dict[int, dict[tuple[PemmdbPlantTypeId, ClusterId], pd.DataFrame]]
    ) -> None:
        root_file_path = self.output_folder / MISC_LOAD_FACTOR_FOLDER
        for year, df_year in index_of_df_year.items():
            for cluster_id, df_cluster in df_year.items():
                file_path = (
                    root_file_path
                    / cluster_id[1]
                    / cluster_id[0]
                    / f"load_factor_{cluster_id[1]}_{year - 1}-{year}.csv"
                )
                write_csv_file(file_path, df_cluster)

    def build_load_factor(self, df_misc_filtered: pd.DataFrame) -> None:
        # parsing index file
        df_index = self._read_input_file()

        # parsing ts file
        df_ts = pd.read_csv(self.input_folder / LOAD_FACTOR_FILE_TS_NAME)

        # treatments for every year
        index_of_df_pegase: dict[int, dict[tuple[PemmdbPlantTypeId, ClusterId], pd.DataFrame]] = {}
        for year in self.years:
            # group index filtered by year + time series (raw data)
            index_mapping_year = self._build_index_mapping_year(df_index, year)

            # structure index and time series dataclass
            index_ts_dataclass_year = InternalIndexTsMapping(index=index_mapping_year, data=df_ts)

            index_cluster_weight = self._build_index_weight_year(df_misc_filtered, year)

            # build dictionary with zone/cluster who contains weighted average time series
            index_ts_weighted_average = self._build_index_ts_weighted_average_year(
                index_ts_dataclass_year, index_cluster_weight
            )

            # final df with Pegase format
            index_cluster_df = self._build_pegase_dataframe(index_ts_weighted_average)
            index_of_df_pegase[year] = index_cluster_df

        self._export_load_factor(index_of_df_pegase)
