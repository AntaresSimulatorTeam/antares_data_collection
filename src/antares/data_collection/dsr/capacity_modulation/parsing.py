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
    ANTARES_NODE_NAME_COLUMN,
    OUTPUT_DATE_INT_REFERENCE,
    SCENARIO_TO_ALWAYS_CONSIDER,
    YearId,
)
from antares.data_collection.dsr.capacity_modulation.constants import (
    DSR_CAPACITY_MODULATION_FOLDER,
    DSR_CAPACITY_MODULATION_NAME_FILE,
    DSR_DERATING_INDEX_NAME,
    DSR_DERATING_NAME,
    DSR_EXPORT_DATE_COLUMN,
    InputDeratingIndexColumns,
)
from antares.data_collection.dsr.constants import InputDsrColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_index_files_with_scenario_year,
    filter_out_based_on_year,
    insert_str_date_time_reindex,
    parse_input_file,
    write_excel_workbook,
)

# mapping used for index file
ZoneId: TypeAlias = str
DeratingId: TypeAlias = str
CurveIds: TypeAlias = list[str]
IndexMapping: TypeAlias = dict[ZoneId, dict[DeratingId, CurveIds]]
DsrClusterId: TypeAlias = tuple[int, int]

# mapping used to add/manage weights calculation
WeightValue: TypeAlias = float
AntaresCodeId: TypeAlias = str
IndexDsrClusterWeight: TypeAlias = dict[AntaresCodeId, dict[DsrClusterId, dict[DeratingId, WeightValue]]]


@dataclass(frozen=True)
class InternalMapping:
    index: IndexMapping
    data: pd.DataFrame


@dataclass
class TimeSeriesAndClusterPair:
    weight: float
    series: pd.Series


DsrWeightTsRepartition: TypeAlias = dict[
    AntaresCodeId, dict[DsrClusterId, dict[DeratingId, list[TimeSeriesAndClusterPair]]]
]


class DsrCapacityModulationParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _build_index_internal_mapping(
        self, df: pd.DataFrame, year: int, cols_to_group: list[str], curve_id_col: str
    ) -> IndexMapping:
        df = filter_index_files_with_scenario_year(
            self.main_params,
            df=df,
            year=year,
            filter_scenario_value=SCENARIO_TO_ALWAYS_CONSIDER,
            target_year_col=InputDeratingIndexColumns.TARGET_YEAR.value,
        )
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: IndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, DeratingId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[curve_id_col])
        return mapping

    def _build_index_mapping(self, df: pd.DataFrame, year: int) -> IndexMapping:
        columns_to_group = [InputDeratingIndexColumns.ZONE.value, InputDeratingIndexColumns.ID.value]
        return self._build_index_internal_mapping(df, year, columns_to_group, InputDeratingIndexColumns.CURVE_UID)

    def _build_index_weight_by_year(self, df: pd.DataFrame, year: YearId) -> IndexDsrClusterWeight:
        # filter data dsr cluster
        df = filter_out_based_on_year(
            df,
            year,
            InputDsrColumns.COMMISSIONING_DATE.value,
            InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        )

        # Define grouping columns based on _compute_dsr_cluster_year
        group_cols = [ANTARES_NODE_NAME_COLUMN, InputDsrColumns.ACT_PRICE_DA, InputDsrColumns.MAX_HOURS]
        subgroup_cols = group_cols + [InputDsrColumns.DSR_DERATING_CURVE_ID]

        # 1. Aggregate capacity by (Area, DSR_cluster, Curve ID)
        df_weights = df.groupby(subgroup_cols, as_index=False)[InputDsrColumns.NET_MAX_GEN_CAP].sum()

        # 2. Compute total capacity per DSR_cluster
        df_cluster_total = (
            df.groupby(group_cols, as_index=False)[[InputDsrColumns.NET_MAX_GEN_CAP]]
            .sum()
            .rename(columns={InputDsrColumns.NET_MAX_GEN_CAP: "total_capacity"})
        )

        # 3. Merge and compute weights
        df_weights = df_weights.merge(df_cluster_total, on=group_cols)
        df_weights[InputDsrColumns.NET_MAX_GEN_CAP] = (
            df_weights[InputDsrColumns.NET_MAX_GEN_CAP] / df_weights["total_capacity"]
        )

        # 3. Structure into a dictionary
        dict_of_weight: IndexDsrClusterWeight = {}
        for _, row in df_weights.iterrows():
            area = row[ANTARES_NODE_NAME_COLUMN]
            dsr_cluster_id = (row[InputDsrColumns.ACT_PRICE_DA], row[InputDsrColumns.MAX_HOURS])
            derating_id = row[InputDsrColumns.DSR_DERATING_CURVE_ID]
            weight = row[InputDsrColumns.NET_MAX_GEN_CAP]

            dict_of_weight.setdefault(area, {}).setdefault(dsr_cluster_id, {})[derating_id] = weight

        return dict_of_weight

    def _build_index_weight_repartition(
        self, index_of_weight: IndexDsrClusterWeight, index_derating_data: InternalMapping
    ) -> DsrWeightTsRepartition:
        """
        Structure data in a dictionary with all data :
            - `weight`: Capacity by area/sector/derating_id / Capacity by zone/sector
            - time series associated to the weight
        Several time series can be found for on index id corresponding to several curves.
        In this case, we compute the mean of the time series.
        If we don't find any time series, we use the value of 1.
        """
        result: DsrWeightTsRepartition = {}

        for zone_id, sectors in index_of_weight.items():
            for sector_id, deratings in sectors.items():
                for derating_id, weight in deratings.items():
                    uid_name = index_derating_data.index.get(zone_id, {}).get(derating_id)

                    if uid_name is None:
                        series_df = pd.DataFrame({"col": [1] * 8760})
                    else:
                        series_df = index_derating_data.data[uid_name]

                    # compute mean of several time series if necessary
                    if len(series_df.columns) > 1:
                        series = series_df.mean(axis=1)
                    else:
                        series = series_df.iloc[:, 0]

                    (
                        result.setdefault(zone_id, {})
                        .setdefault(sector_id, {})
                        .setdefault(derating_id, [])
                        .append(TimeSeriesAndClusterPair(weight=weight, series=series))
                    )

        return result

    def _build_pegase_dataframe(self, data_repartition: DsrWeightTsRepartition) -> pd.DataFrame:
        result: dict[str, pd.Series] = {}

        for area, dsrclusters in data_repartition.items():
            i = 1
            for dsrcluster_id, deratings in dsrclusters.items():
                for derating_id, pair_of_weights in deratings.items():
                    ts_value = pair_of_weights[0].series * pair_of_weights[0].weight
                    ts_name = f"{area}_DSR{i}"
                    result[ts_name] = ts_value
                i += 1

        df_result = pd.DataFrame(result)

        # Add the Hours columns
        reindex_df = insert_str_date_time_reindex(df_result, OUTPUT_DATE_INT_REFERENCE, DSR_EXPORT_DATE_COLUMN)

        return reindex_df

    def _export_dsr_capacity_modulation_dataframe(self, index_of_df_year: dict[int, pd.DataFrame]) -> None:
        parent_dir = self.output_folder / DSR_CAPACITY_MODULATION_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / DSR_CAPACITY_MODULATION_NAME_FILE

        dict_to_write: dict[str, pd.DataFrame] = {}
        for year, df_year in index_of_df_year.items():
            sheet_name = f"{year - 1}-{year}"
            dict_to_write[sheet_name] = df_year

        write_excel_workbook(output_path, dict_to_write)

    def _parse_derating_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / DSR_DERATING_INDEX_NAME, list(InputDeratingIndexColumns))

    def build_dsr_capacity_modulation(self, df_dsr_cluster_filtered: pd.DataFrame) -> None:
        # parsing index file
        dsr_derating_index_df = self._parse_derating_index()

        # parsing ts file
        dsr_derating_ts_df = pd.read_csv(self.input_folder / DSR_DERATING_NAME)

        # treatments for every year
        index_of_df_pegase: dict[int, pd.DataFrame] = {}
        for year in self.years:
            # group index filtered by year + time series (raw data)
            index_mapping_year = self._build_index_mapping(dsr_derating_index_df, year)
            derating_index_data = InternalMapping(index=index_mapping_year, data=dsr_derating_ts_df)

            # buil dictionary with weight by sector/derating_id
            index_cluster_id_weight = self._build_index_weight_by_year(df_dsr_cluster_filtered, year)

            # group all data DSR + index/ts in a dictionary
            index_repartition_weight_ts = self._build_index_weight_repartition(
                index_cluster_id_weight, derating_index_data
            )

            # final treatments to have data frame
            df_ts_area_sector = self._build_pegase_dataframe(index_repartition_weight_ts)

            index_of_df_pegase[year] = df_ts_area_sector

        # write the capacity modulation file
        self._export_dsr_capacity_modulation_dataframe(index_of_df_pegase)
