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

from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS, YearId
from antares.data_collection.dsr.constants import (
    DSR_DERATING_INDEX_NAME,
    DSR_DERATING_NAME,
    DSR_FO_DURATION,
    DSR_FO_RATE,
    DSR_FOLDER,
    DSR_GROUP,
    DSR_INPUT_FILE,
    DSR_NAME_FILE,
    DSR_NB_HOUR_PER_DAY,
    InputDeratingIndexColumns,
    InputDsrColumns,
    OutputDsrColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.param_modulation.constants import SCENARIO_TO_ALWAYS_CONSIDER
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    parse_input_file,
)

ZoneId: TypeAlias = str
DeratingId: TypeAlias = str
CurveIds: TypeAlias = list[str]
IndexMapping: TypeAlias = dict[ZoneId, dict[DeratingId, CurveIds]]
SectorId: TypeAlias = str

@dataclass(frozen=True)
class InternalMapping:
    index: IndexMapping
    data: pd.DataFrame

@dataclass
class TimeSeriesAndClusterPair:
    weight: float
    series: pd.Series
    should_write_the_series: bool

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

        date = pd.Timestamp(year=year, month=1, day=1)

        # Filter active assets
        mask = (df[InputDsrColumns.COMMISSIONING_DATE] <= date) & (
            df[InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED] >= date
        )

        df_year = df.loc[mask]

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

    def _filter_columns_for_output(self, dict_of_df: dict[int, pd.DataFrame]) -> dict[int, pd.DataFrame]:
        """
        Only keep the output columns needed for the DSR cluster.
            - Ordering columns from `OutputDsrColumns`
        """
        columns_to_keep = [col.value for col in OutputDsrColumns]
        return {year: df[columns_to_keep] for year, df in dict_of_df.items()}

    def _export_dsr_cluster_dataframe(self, dict_of_df: dict[int, pd.DataFrame]) -> None:
        parent_dir = self.output_folder / DSR_FOLDER
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

    def _filter_index_files_with_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        scenario = self.main_params.get_scenario_type(year=year)
        acceptable_scenario_types = [SCENARIO_TO_ALWAYS_CONSIDER, f"{scenario}_{year}", f"All_years_{scenario}"]
        return df[df[InputDeratingIndexColumns.TARGET_YEAR].isin(acceptable_scenario_types)]

    def _build_index_mapping(self, df: pd.DataFrame, year: int) -> IndexMapping:
        columns_to_group = [InputDeratingIndexColumns.ZONE.value, InputDeratingIndexColumns.ID.value]
        return self._build_index_internal_mapping(df, year, columns_to_group, InputDeratingIndexColumns.CURVE_UID)

    def _build_index_internal_mapping(
            self, df: pd.DataFrame, year: int, cols_to_group: list[str], curve_id_col: str
    ) -> IndexMapping:
        df = self._filter_index_files_with_year(df=df, year=year)
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: IndexMapping = {}
        for (area, cluster), grouped_df in groups:
            assert isinstance(area, ZoneId)
            assert isinstance(cluster, DeratingId)
            mapping.setdefault(area, {})[cluster] = list(grouped_df[curve_id_col])
        return mapping

    def _build_weight_by_sector_derating_id_year(self, df: pd.DataFrame, year: YearId) -> pd.DataFrame:
        # filter data dsr cluster
        date = pd.Timestamp(year=year, month=1, day=1)

        # Filter active assets
        mask = (df[InputDsrColumns.COMMISSIONING_DATE] <= date) & (
                df[InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED] >= date
        )

        df_dsr_cluster_year = df.loc[mask]

        dsr_cluster_sector_cols = [
            ANTARES_NODE_NAME_COLUMN,
            InputDsrColumns.SECTOR,
            InputDsrColumns.NET_MAX_GEN_CAP,
        ]

        # compute dsr capacity by sector
        df_aggregate_sector = (
            df_dsr_cluster_year[dsr_cluster_sector_cols]
            .groupby([ANTARES_NODE_NAME_COLUMN,
                      InputDsrColumns.SECTOR], as_index=False)
            .sum()
            .rename(columns={InputDsrColumns.NET_MAX_GEN_CAP: "sector_capacity"})
        )

        # compute dsr capacity by curve id
        dsr_cluster_derating_id_cols = [
            ANTARES_NODE_NAME_COLUMN,
            InputDsrColumns.SECTOR,
            InputDsrColumns.DSR_DERATING_CURVE_ID,
            InputDsrColumns.NET_MAX_GEN_CAP,
        ]

        df_aggregate_sector_derating_id = (
            df_dsr_cluster_year[dsr_cluster_derating_id_cols]
            .groupby([ANTARES_NODE_NAME_COLUMN,
                      InputDsrColumns.SECTOR,
                      InputDsrColumns.DSR_DERATING_CURVE_ID],
                     as_index=False)
            .sum()
            .rename(columns={InputDsrColumns.NET_MAX_GEN_CAP: "derating_id_capacity"})
        )

        # compute the final weight for every area/derating_id
        df_weight_of_area_sector_derating = (
            df_aggregate_sector_derating_id
            .merge(
                df_aggregate_sector,
                on=[ANTARES_NODE_NAME_COLUMN, InputDsrColumns.SECTOR]
            )
            .assign(
                weight=lambda df: (
                        df["derating_id_capacity"] / df["sector_capacity"]
                )
            )
        )

        col_to_to_keep = [ANTARES_NODE_NAME_COLUMN, InputDsrColumns.SECTOR, InputDsrColumns.DSR_DERATING_CURVE_ID, "weight"]
        return df_weight_of_area_sector_derating[col_to_to_keep]


    def _build_dsr_capacity_modulation(self, df_dsr_cluster_filtered: pd.DataFrame):
        # parsing index file
        dsr_derating_index_df = self._parse_derating_index()

        # parsing ts file
        dsr_derating_ts_df = pd.read_csv(self.input_folder / DSR_DERATING_NAME)

        # treatments for every year

        # filter df dsr_cluster
        # grouping by area/SECTOR
        # sum of capacity (NET_MAX_GEN_CAP)

        # filter df dsr_derating_index on year
        # mapping with df aggregate and df dsr_derating_index (DERATING_INDEX_ID)
        # mapping with df dsr_derating DERATING_ID/DERATING_VALUE/DERATING_INDEX_ID
        # compute mean of DERATING_VALUE TS if multi row CURVE_UID/ZONE/ID

        for year in self.years:

            # group index filtered by year + time series (raw data)
            index_mapping_year = self._build_index_mapping(dsr_derating_index_df, year)
            derating_index_data = InternalMapping(index=index_mapping_year, data=dsr_derating_ts_df)


            # filter data dsr cluster for a year
            date = pd.Timestamp(year=year, month=1, day=1)

            # filter active assets
            mask = (df_dsr_cluster_filtered[InputDsrColumns.COMMISSIONING_DATE] <= date) & (
                    df_dsr_cluster_filtered[InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED] >= date
            )

            df_dsr_cluster_year = df_dsr_cluster_filtered.loc[mask]

            df_cluster_id_weight = self._build_weight_by_sector_derating_id_year(df_dsr_cluster_year, year)

            # try mapping
            dict_of_weight: dict[ZoneId, dict[SectorId, dict[DeratingId, float]]] = {}
            for (area, sector, derating_id), weight in df_cluster_id_weight.groupby(
                    [ANTARES_NODE_NAME_COLUMN, InputDsrColumns.SECTOR, InputDsrColumns.DSR_DERATING_CURVE_ID],
                    as_index=False,
            ):
                dict_of_weight.setdefault(area, {}).setdefault(sector, {})[derating_id] = weight["weight"].iloc[0]

            # apply to time series the weight from aggregation
            derating_index_filtered = {k: v for k, v in dict_of_weight.items() if k in derating_index_data.index}



            # # Write the capacity modulation file
            # dsr_cluster_group_ts_repartition = self._build_must_run(thermal_df_year, index_to_timeseries)
            # self._write_must_run_file(year, must_run_cluster_group_ts_repartition)

    def build_dsr_cluster(self) -> None:
        df_filtered = self._build_filtered_dsr_cluster_dataframe()
        dict_of_df = self._compute_dsr_cluster_years(df_filtered)
        dict_of_df = self._filter_columns_for_output(dict_of_df)
        self._export_dsr_cluster_dataframe(dict_of_df)

        # capacity modulation
        self._build_dsr_capacity_modulation(df_filtered)
