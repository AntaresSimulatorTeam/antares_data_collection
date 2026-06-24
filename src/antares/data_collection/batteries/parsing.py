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
    GROUP_VALUES,
    OP_STAT_MARKET,
    OP_STAT_RESIDENTIAL,
    PEMMDB_PLANT_TYPE_MARKET,
    PEMMDB_PLANT_TYPE_RESIDENTIAL,
    InputBatteriesColumns,
    OutputBatteriesColumns,
)
from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
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
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.pemmdb_plant_type_market = pemmdb_plant_type_market
        self.op_stat_market = op_stat_market
        self.pemmdb_plant_type_residential = pemmdb_plant_type_residential
        self.op_stat_residential = op_stat_residential
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

    def _compute_batteries_duration_capacities(self, df: pd.DataFrame) -> IndexStockDuration:
        result: IndexStockDuration = {}
        for _, row in df.iterrows():
            hour_stock = row.STO_CAP / row.NET_MAX_CAP_GEN
            result[row[ANTARES_NODE_NAME_COLUMN]] = hour_stock

        return result

    def _build_pegase_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # duration capacity
        DURATION_CAPACITY_COL = "HOUR_STO"
        df[DURATION_CAPACITY_COL] = round(df[InputBatteriesColumns.STO_CAP] / df[InputBatteriesColumns.NET_MAX_CAP_GEN])

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
        grouped_df = (
            df.groupby([ANTARES_NODE_NAME_COLUMN, OutputBatteriesColumns.GROUP, DURATION_CAPACITY_COL])
            .agg(**{OutputBatteriesColumns.EFFICIENCY_INJECTION.value: (InputBatteriesColumns.NET_MAX_CAP_DEM, "sum")})
            .reset_index()
        )

        return grouped_df

    def build_batteries(self) -> None:
        df = self._build_filtered_batteries_dataframe()
        # index_stock_duaration = self._compute_batteries_duration_capacities(df)
        df = self._build_pegase_dataframe(df)
