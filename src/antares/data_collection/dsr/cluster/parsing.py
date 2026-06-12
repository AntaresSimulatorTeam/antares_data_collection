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

import pandas as pd

from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS, YearId
from antares.data_collection.dsr.cluster.constants import (
    DSR_CLUSTER_FOLDER,
    DSR_FO_DURATION,
    DSR_FO_RATE,
    DSR_GROUP,
    DSR_NAME_FILE,
    DSR_NB_HOUR_PER_DAY,
    DSR_PRICE_VALUE,
    OutputDsrColumns,
)
from antares.data_collection.dsr.constants import DSR_INDEX_GROUP_COLUMNS, InputDsrColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import filter_out_based_on_year


class DsrClusterParser:
    def __init__(self, output_folder: Path, main_params: MainParams, years: list[int]):
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

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

        # weights
        col_name_weights = "_weighted_max_hours"
        df_year[col_name_weights] = df_year[InputDsrColumns.MAX_HOURS] * df_year[InputDsrColumns.NET_MAX_GEN_CAP]

        # Group with aggregations
        result = (
            df_year.groupby(DSR_INDEX_GROUP_COLUMNS, as_index=False)
            .agg(
                **{
                    OutputDsrColumns.CAPACITY.value: (InputDsrColumns.NET_MAX_GEN_CAP, "sum"),
                    OutputDsrColumns.NB_UNITS.value: (InputDsrColumns.NET_MAX_GEN_CAP, "size"),
                    OutputDsrColumns.MODULATION.value: (
                        InputDsrColumns.DSR_DERATING_CURVE_ID,
                        lambda x: x.notna().any(),
                    ),
                    OutputDsrColumns.MAX_HOUR_PER_DAY.value: (col_name_weights, "sum"),
                }
            )
            .rename(columns={ANTARES_NODE_NAME_COLUMN: OutputDsrColumns.AREA})
        )

        # normalize to have weighted average
        result[OutputDsrColumns.MAX_HOUR_PER_DAY] = (
            result[OutputDsrColumns.MAX_HOUR_PER_DAY] / result[OutputDsrColumns.CAPACITY]
        ).round()

        # build column "NAME" by AREA put "{ZONE}_DSR"
        result[OutputDsrColumns.NAME] = result[OutputDsrColumns.AREA] + "_" + DSR_GROUP

        # Round capacities
        result[OutputDsrColumns.CAPACITY] = result[OutputDsrColumns.CAPACITY].round(MAX_DECIMAL_DIGITS)

        # Add static columns
        result[OutputDsrColumns.TO_USE] = 1
        result[OutputDsrColumns.GROUP] = DSR_GROUP
        result[OutputDsrColumns.NB_HOUR_PER_DAY] = DSR_NB_HOUR_PER_DAY
        result[OutputDsrColumns.PRICE] = DSR_PRICE_VALUE
        result[OutputDsrColumns.FO_RATE] = DSR_FO_RATE
        result[OutputDsrColumns.FO_DURATION] = DSR_FO_DURATION

        # return with business columns order
        return result[list(OutputDsrColumns)]

    def _compute_dsr_cluster_years(self, df: pd.DataFrame) -> dict[YearId, pd.DataFrame]:
        years = sorted(self.years)

        res: dict[YearId, pd.DataFrame] = {}
        for year in years:
            res[year] = self._compute_dsr_cluster_year(df, year)

        return res

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

    # capacity of DSR clustering
    def build_dsr_cluster(self, df: pd.DataFrame) -> None:
        index_of_df_year = self._compute_dsr_cluster_years(df)
        self._export_dsr_cluster_dataframe(index_of_df_year)
