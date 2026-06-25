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

import numpy as np
import pandas as pd

from antares.data_collection.batteries.constants import (
    BATTERIES_INPUT_FILE,
    DEFAULT_CONSTRAINTS,
    DEFAULT_EFFICIENCY_WITHDRAWAL,
    DEFAULT_INITIAL_LEVEL,
    DEFAULT_INITIAL_LEVEL_OPTIM,
    DEFAULT_NAME,
    DEFAULT_SERIES,
    EFFICIENCY_INJECTION,
    GROUP_VALUES,
    OP_STAT_MARKET,
    OP_STAT_RESIDENTIAL,
    PEMMDB_PLANT_TYPE_MARKET,
    PEMMDB_PLANT_TYPE_RESIDENTIAL,
    InputBatteriesColumns,
    OutputBatteriesColumns,
)
from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS, YearId
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    filter_out_based_on_year,
    parse_input_file,
)

AntaresNodeId: TypeAlias = str
IndexStockDuration: TypeAlias = dict[AntaresNodeId, float]


class BatteriesParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        main_params: MainParams,
        years: list[int],
        pemmdb_plant_type_market: list[str] = PEMMDB_PLANT_TYPE_MARKET,
        op_stat_market: list[str] = OP_STAT_MARKET,
        pemmdb_plant_type_residential: list[str] = PEMMDB_PLANT_TYPE_RESIDENTIAL,
        op_stat_residential: list[str] = OP_STAT_RESIDENTIAL,
        efficiency_injection: float = EFFICIENCY_INJECTION,
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.pemmdb_plant_type_market = pemmdb_plant_type_market
        self.op_stat_market = op_stat_market
        self.pemmdb_plant_type_residential = pemmdb_plant_type_residential
        self.op_stat_residential = op_stat_residential
        self.efficiency_injection = efficiency_injection
        self.years = years
        self.filtered_dataframe = self._build_filtered_batteries_dataframe()

    def _read_input_file_batteries(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(BATTERIES_INPUT_FILE), list(InputBatteriesColumns))

    def _build_filtered_batteries_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_batteries()
        df = filter_non_declared_areas(self.main_params, df, InputBatteriesColumns.MARKET_NODE)
        df = filter_based_on_study_scenarios(df, self.main_params, self.years, InputBatteriesColumns.STUDY_SCENARIO)
        df = filter_based_on_commission_date(
            df,
            self.years,
            InputBatteriesColumns.COMMISSIONING_DATE,
            InputBatteriesColumns.DECOMMISSIONING_DATE_EXPECTED,
        )
        df = filter_based_on_op_stat(self.op_stat_market + self.op_stat_residential, df, InputBatteriesColumns.OP_STAT)
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.NET_MAX_CAP_GEN)
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.NET_MAX_CAP_DEM)
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.STO_CAP)
        df = add_code_antares_colum(self.main_params, df, InputBatteriesColumns.MARKET_NODE)

        return df

    def _compute_aggregated_columns_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        # filter for a year
        df = filter_out_based_on_year(
            df, year, InputBatteriesColumns.COMMISSIONING_DATE, InputBatteriesColumns.DECOMMISSIONING_DATE_EXPECTED
        )

        # duration capacity
        DURATION_CAPACITY_COL = "HOUR_STO"
        df[DURATION_CAPACITY_COL] = (
            df[InputBatteriesColumns.STO_CAP] / df[InputBatteriesColumns.NET_MAX_CAP_GEN]
        ).round()

        # Define conditions
        conditions = [
            # market
            (df[InputBatteriesColumns.OP_STAT].isin(self.op_stat_market))
            & (df[InputBatteriesColumns.PEMMDB_PLANT_TYPE].isin(self.pemmdb_plant_type_market)),
            # residential
            (df[InputBatteriesColumns.OP_STAT].isin(self.op_stat_residential))
            & (df[InputBatteriesColumns.PEMMDB_PLANT_TYPE].isin(self.pemmdb_plant_type_residential)),
        ]

        # Define corresponding values from GROUP_VALUES
        choices = [GROUP_VALUES[0], GROUP_VALUES[1]]

        # create GROUP column
        df[OutputBatteriesColumns.GROUP] = np.select(conditions, choices, default="unknown")

        # Group by multiple columns and sum
        aggregate_df = (
            df.groupby([ANTARES_NODE_NAME_COLUMN, OutputBatteriesColumns.GROUP, DURATION_CAPACITY_COL])
            .agg(
                **{
                    OutputBatteriesColumns.INJECTION.value: (InputBatteriesColumns.NET_MAX_CAP_DEM, "sum"),
                    OutputBatteriesColumns.WITHDRAWAL.value: (InputBatteriesColumns.NET_MAX_CAP_GEN, "sum"),
                    OutputBatteriesColumns.STORAGE.value: (InputBatteriesColumns.STO_CAP, "sum"),
                }
            )
            .round(MAX_DECIMAL_DIGITS)
            .reset_index()
            .rename(columns={ANTARES_NODE_NAME_COLUMN: OutputBatteriesColumns.AREA})
        )

        # build column "NAME"
        # "battery"
        name_with_duration = (
            aggregate_df[OutputBatteriesColumns.GROUP].astype(str)
            + "_"
            + aggregate_df[DURATION_CAPACITY_COL].astype(int).astype(str)
        )

        aggregate_df[OutputBatteriesColumns.NAME] = np.where(
            aggregate_df[OutputBatteriesColumns.GROUP] == GROUP_VALUES[0], name_with_duration, DEFAULT_NAME
        )

        # Add static columns
        aggregate_df[OutputBatteriesColumns.EFFICIENCY_INJECTION] = self.efficiency_injection
        aggregate_df[OutputBatteriesColumns.EFFICIENCY_WITHDRAWAL] = DEFAULT_EFFICIENCY_WITHDRAWAL
        aggregate_df[OutputBatteriesColumns.INITIAL_LEVEL] = DEFAULT_INITIAL_LEVEL
        aggregate_df[OutputBatteriesColumns.INITIAL_LEVEL_OPTIM] = DEFAULT_INITIAL_LEVEL_OPTIM

        aggregate_df[OutputBatteriesColumns.ENABLED] = np.where(
            aggregate_df[OutputBatteriesColumns.GROUP] == GROUP_VALUES[0], True, False
        )

        aggregate_df[OutputBatteriesColumns.SERIES] = DEFAULT_SERIES
        aggregate_df[OutputBatteriesColumns.CONSTRAINTS] = DEFAULT_CONSTRAINTS

        return aggregate_df[list(OutputBatteriesColumns)]

    # def _export_batteries(self, dict_of_df: dict[int, pd.DataFrame]) -> None:
    #     parent_dir = self.output_folder / BATTERIES_FOLDER
    #     parent_dir.mkdir(parents=True, exist_ok=True)
    #
    #     output_path = parent_dir / DSR_NAME_FILE
    #
    #     with pd.ExcelWriter(output_path) as writer:
    #         for year, df in dict_of_df.items():
    #             sheet_name = str(year)
    #
    #             df.to_excel(
    #                 writer,
    #                 sheet_name=sheet_name,
    #                 index=False,
    #             )

    def build_batteries(self) -> None:
        df = self._build_filtered_batteries_dataframe()

        years = sorted(self.years)
        res: dict[YearId, pd.DataFrame] = {}
        for year in years:
            res[year] = self._compute_aggregated_columns_year(df, year)
