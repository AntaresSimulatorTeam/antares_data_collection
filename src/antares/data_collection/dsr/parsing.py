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

import numpy as np
import pandas as pd

from antares.data_collection.constants import (
    ANTARES_NODE_NAME_COLUMN,
    MAX_DECIMAL_DIGITS,
    SCENARIO_TO_ALWAYS_CONSIDER,
    YearId,
)
from antares.data_collection.dsr.constants import (
    DSR_CAPACITY_MODULATION_FOLDER,
    DSR_CAPACITY_MODULATION_NAME_FILE,
    DSR_CLUSTER_FOLDER,
    DSR_DATE_INT_REFERENCE,
    DSR_DERATING_INDEX_NAME,
    DSR_DERATING_NAME,
    DSR_EXPORT_DATE_COLUMN,
    DSR_FO_DURATION,
    DSR_FO_RATE,
    DSR_GROUP,
    DSR_INPUT_FILE,
    DSR_NAME_FILE,
    DSR_NB_HOUR_PER_DAY,
    InputDeratingIndexColumns,
    InputDsrColumns,
    OutputDsrColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    _filter_index_files_with_year,
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    filter_out_based_on_year,
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


class DsrParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        op_stat_values: list[str],
        dsr_type_values: list[str],
        act_price_da: list[int],
        main_params: MainParams,
        years: list[int],
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.op_stat_values = op_stat_values
        self.dsr_type_values = dsr_type_values
        self.act_price_da = act_price_da
        self.main_params = main_params
        self.years = years

    def _read_input_file_dsr_cluster(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(DSR_INPUT_FILE), list(InputDsrColumns))

    def _parse_derating_index(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / DSR_DERATING_INDEX_NAME, list(InputDeratingIndexColumns))

    def _filter_based_on_dsr_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """We want to keep only the lines where the DSR_TYPE value matches the user given ones"""
        dsr_type_values = self.dsr_type_values
        if not dsr_type_values:
            return df
        df = df[df[InputDsrColumns.DSR_TYPE].isin(dsr_type_values)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"The given dsr_type values {dsr_type_values} are not present in the dataframe")
        return df

    def _filter_out_based_on_act_price_da(self, df: pd.DataFrame) -> pd.DataFrame:
        """We want to exclude only the lines where the ACT_PRICE_DA value matches the user given ones"""
        act_price_da = self.act_price_da
        if not act_price_da:
            return df
        df = df[~df[InputDsrColumns.ACT_PRICE_DA].isin(act_price_da)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"The given act_price_da values {act_price_da} exclude all row in the dataframe")
        return df

    def _compute_dsr_cluster_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Compute DSR metrics for a given year:
            - sum of capacity
            - number of units
            - modulation flag
            - add new columns
            - naming convention from `OutputDsrColumns`
            - ordering columns from `OutputDsrColumns`
        """

        df_year = filter_out_based_on_year(
            df, year, InputDsrColumns.COMMISSIONING_DATE.value, InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED.value
        )

        # Group with aggregations
        result = (
            df_year.groupby(
                [ANTARES_NODE_NAME_COLUMN, InputDsrColumns.ACT_PRICE_DA, InputDsrColumns.MAX_HOURS], as_index=False
            )
            .agg(
                **{
                    OutputDsrColumns.CAPACITY.value: (InputDsrColumns.NET_MAX_GEN_CAP, "sum"),
                    OutputDsrColumns.NB_UNITS.value: (InputDsrColumns.NET_MAX_GEN_CAP, "size"),
                    OutputDsrColumns.MODULATION.value: (
                        InputDsrColumns.DSR_DERATING_CURVE_ID,
                        lambda x: x.notna().any(),
                    ),
                }
            )
            .rename(columns={ANTARES_NODE_NAME_COLUMN: OutputDsrColumns.AREA})
        )

        # build column "NAME" by AREA put "DSR{1:N}"
        result[OutputDsrColumns.NAME] = DSR_GROUP + (result.groupby(OutputDsrColumns.AREA).cumcount() + 1).astype(str)

        # Round capacities
        result[OutputDsrColumns.CAPACITY] = result[OutputDsrColumns.CAPACITY].round(MAX_DECIMAL_DIGITS)

        # Add static columns
        result[OutputDsrColumns.TO_USE] = 1
        result[OutputDsrColumns.GROUP] = DSR_GROUP
        result[OutputDsrColumns.NB_HOUR_PER_DAY] = DSR_NB_HOUR_PER_DAY
        result[OutputDsrColumns.MAX_HOUR_PER_DAY] = np.where(
            result[InputDsrColumns.MAX_HOURS] == 0, DSR_NB_HOUR_PER_DAY, result[InputDsrColumns.MAX_HOURS]
        )
        result[OutputDsrColumns.PRICE] = result[InputDsrColumns.ACT_PRICE_DA]
        result[OutputDsrColumns.FO_RATE] = DSR_FO_RATE
        result[OutputDsrColumns.FO_DURATION] = DSR_FO_DURATION

        return result

    def _compute_dsr_cluster_years(self, df: pd.DataFrame) -> dict[YearId, pd.DataFrame]:
        years = sorted(self.years)

        res: dict[YearId, pd.DataFrame] = {}
        for year in years:
            res[year] = self._compute_dsr_cluster_year(df, year)

        return res

    def _filter_columns_for_output(self, dict_of_df: dict[YearId, pd.DataFrame]) -> dict[YearId, pd.DataFrame]:
        """
        Only keep the output columns needed for the DSR cluster.
            - Ordering columns from `OutputDsrColumns`
        """
        columns_to_keep = [col.value for col in OutputDsrColumns]
        return {year: df[columns_to_keep] for year, df in dict_of_df.items()}

    def _export_dsr_cluster_dataframe(self, dict_of_df: dict[int, pd.DataFrame]) -> None:
        parent_dir = self.output_folder / DSR_CLUSTER_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / DSR_NAME_FILE

        with pd.ExcelWriter(output_path) as writer:
            for year, df in dict_of_df.items():
                sheet_name = str(year)

                df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                )

    def _build_filtered_dsr_cluster_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_dsr_cluster()
        df = filter_based_on_op_stat(self.op_stat_values, df, InputDsrColumns.OP_STAT.value)
        df = self._filter_based_on_dsr_type(df)
        df = self._filter_out_based_on_act_price_da(df)
        df = filter_non_declared_areas(self.main_params, df, InputDsrColumns.MARKET_NODE.value)
        df = filter_based_on_study_scenarios(df, self.main_params, self.years, InputDsrColumns.STUDY_SCENARIO.value)
        df = filter_based_on_commission_date(
            df,
            self.years,
            InputDsrColumns.COMMISSIONING_DATE.value,
            InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        )
        df = filter_based_on_net_max_gen_cap(df, InputDsrColumns.NET_MAX_GEN_CAP.value)
        df = add_code_antares_colum(self.main_params, df, InputDsrColumns.MARKET_NODE.value)

        return df

    def _build_index_mapping(self, df: pd.DataFrame, year: int) -> IndexMapping:
        columns_to_group = [InputDeratingIndexColumns.ZONE.value, InputDeratingIndexColumns.ID.value]
        return self._build_index_internal_mapping(df, year, columns_to_group, InputDeratingIndexColumns.CURVE_UID)

    def _build_index_internal_mapping(
        self, df: pd.DataFrame, year: int, cols_to_group: list[str], curve_id_col: str
    ) -> IndexMapping:
        df = _filter_index_files_with_year(
            self.main_params, df=df, year=year, filter_scenario_value=SCENARIO_TO_ALWAYS_CONSIDER
        )
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: IndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, DeratingId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[curve_id_col])
        return mapping

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
        df_weights = df.groupby(subgroup_cols, as_index=False, dropna=False)[InputDsrColumns.NET_MAX_GEN_CAP].sum()

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

        # Add the "date" columns
        all_name_columns = list(df_result.columns)
        start_time = pd.to_datetime(f"01/01/{DSR_DATE_INT_REFERENCE} 00:00:00")
        df_result[DSR_EXPORT_DATE_COLUMN] = [str(start_time + pd.Timedelta(hours=i)) for i in range(len(df_result))]

        # re order columns
        df_result = df_result[[DSR_EXPORT_DATE_COLUMN] + all_name_columns]

        # We want our dataframe to start on the 1st of July at midnight for PEGASE.
        # So we have to reindex it at the right index
        year_int = DSR_DATE_INT_REFERENCE
        time_delta = pd.Timestamp(year=year_int, month=7, day=1, hour=0) - pd.Timestamp(
            year=year_int, month=1, day=1, hour=0
        )
        first_index = time_delta.days * 24
        new_index = list(range(first_index, len(df_result))) + list(range(0, first_index))

        return df_result.reindex(new_index)

    def _export_dsr_capacity_modulation_dataframe(self, index_of_df_year: dict[int, pd.DataFrame]) -> None:
        parent_dir = self.output_folder / DSR_CAPACITY_MODULATION_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / DSR_CAPACITY_MODULATION_NAME_FILE

        dict_to_write: dict[str, pd.DataFrame] = {}
        for year, df_year in index_of_df_year.items():
            sheet_name = f"{year - 1}-{year}"
            dict_to_write[sheet_name] = df_year
            write_excel_workbook(output_path, dict_to_write)

    def _build_dsr_capacity_modulation(self, df_dsr_cluster_filtered: pd.DataFrame) -> None:
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

    def build_dsr_cluster(self) -> None:
        df_filtered = self._build_filtered_dsr_cluster_dataframe()
        dsr_clusters_by_years = self._compute_dsr_cluster_years(df_filtered)
        dsr_clusters_by_years = self._filter_columns_for_output(dsr_clusters_by_years)
        self._export_dsr_cluster_dataframe(dsr_clusters_by_years)

        # capacity modulation
        self._build_dsr_capacity_modulation(df_filtered)
