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

import numpy as np
import pandas as pd

from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS, YearId
from antares.data_collection.dsr.cluster.constants import (
    DSR_CLUSTER_FOLDER,
    DSR_FO_DURATION,
    DSR_FO_RATE,
    DSR_GROUP,
    DSR_NAME_FILE,
    DSR_NB_HOUR_PER_DAY,
    OutputDsrColumns,
)
from antares.data_collection.dsr.constants import InputDsrColumns
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

    # capacity of DSR clustering
    def build_dsr_cluster(self, df: pd.DataFrame) -> None:
        dsr_clusters_by_years = self._compute_dsr_cluster_years(df)
        dsr_clusters_by_years = self._filter_columns_for_output(dsr_clusters_by_years)
        self._export_dsr_cluster_dataframe(dsr_clusters_by_years)
