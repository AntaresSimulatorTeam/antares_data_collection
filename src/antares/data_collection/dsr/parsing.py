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

from antares.data_collection.dsr.constants import DSR_INPUT_FILE, InputDsrColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import ANTARES_NODE_NAME_COLUMN
from antares.data_collection.thermal.utils import (
    add_code_antares_colum,
    filter_df_values_based_on_op_stat,
    filter_input_based_on_study_scenarios,
    filter_non_declared_areas,
    filter_thermal_input_file_based_on_commission_date,
    filter_values_based_on_net_max_gen_cap,
    parse_input_file,
)
from antares.data_collection.utils import MAX_DECIMAL_DIGITS


class DsrParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        op_stat_values: list[str],
        main_params: MainParams,
        years: list[int],
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.op_stat_values = op_stat_values
        self.main_params = main_params
        self.years = years

    def _read_input_file_dsr_cluster(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(DSR_INPUT_FILE), list(InputDsrColumns))

    def _compute_dsr_cluster_year(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Compute DSR metrics for a given year:
        - sum of capacity
        - number of units
        - modulation flag
        """

        date = pd.Timestamp(year=year, month=1, day=1)

        # Filter active assets
        mask = (df[InputDsrColumns.COMMISSIONING_DATE] <= date) & (
            df[InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED] >= date
        )

        df_year = df.loc[mask]

        # Group with aggregations
        result = df_year.groupby(
            [ANTARES_NODE_NAME_COLUMN, InputDsrColumns.ACT_PRICE_DA, InputDsrColumns.MAX_HOURS], as_index=False
        ).agg(
            capacity=(InputDsrColumns.NET_MAX_GEN_CAP, "sum"),
            nb_units=(InputDsrColumns.NET_MAX_GEN_CAP, "size"),
            modulation=(InputDsrColumns.DSR_DERATING_CURVE_ID, lambda x: x.notna().any()),
        )

        # Round capacity
        result["capacity"] = result["capacity"].round(MAX_DECIMAL_DIGITS)

        # Add year
        result["year"] = year

        return result

    def _compute_dsr_cluster_years(self, df: pd.DataFrame) -> pd.DataFrame:
        years = sorted(self.years)

        res: dict[int, pd.DataFrame] = {}
        for year in years:
            res[year] = self._compute_dsr_cluster_year(df, year)

        # date_ranges = [pd.Timestamp(year=year, month=1, day=1) for year in years]
        #
        # # columns needed for the computation
        # start_dates = list(df[InputDsrColumns.COMMISSIONING_DATE])
        # end_dates = list(df[InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED])
        # capacities = list(df[InputDsrColumns.NET_MAX_GEN_CAP])
        # act_prices = list(df[InputDsrColumns.ACT_PRICE_DA])
        # max_hours = list(df[InputDsrColumns.MAX_HOURS])
        # node_names = list(df[ANTARES_NODE_NAME_COLUMN])
        #
        # dsr_groups: dict[str, dict[int, dict[pd.Timestamp, list[int | float]]]] = {}
        #
        # # build the dsr_groups dict with capacities
        # for k in range(len(df)):
        #     antares_node = node_names[k]
        #     act_price = act_prices[k]
        #     max_hour = max_hours[k]
        #
        #     (dsr_groups
        #      .setdefault(antares_node, {})
        #      .setdefault(act_price, {})
        #      .setdefault(max_hour, {}))
        #
        #     for date_range in date_ranges:
        #         if start_dates[k] <= date_range <= end_dates[k]:
        #             (dsr_groups[antares_node][act_price][max_hour]
        #              .setdefault(date_range, [])
        #              .append(capacities[k]))
        #
        # # structure of output data
        # output_data: dict[str, list[Any]] = {
        #     OutputDsrColumns.AREA: [],
        #     # OutputDsrColumns.NAME: [],
        #     OutputDsrColumns.GROUP: [],
        #     # OutputDsrColumns.CAPACITY: [],
        #     # OutputDsrColumns.NB_HOUR_PER_DAY: [],
        #     # OutputDsrColumns.MAX_HOUR_PER_DAY: [],
        #     # OutputDsrColumns.PRICE: [],
        #     # OutputDsrColumns.NB_UNITS: [],
        #     # OutputDsrColumns.FO_RATE: [],
        #     # OutputDsrColumns.FO_DURATION: [],
        #     # OutputDsrColumns.MODULATION: [],
        # }
        #
        # for date_range in date_ranges:
        #     year_as_string = date_range.strftime("%Y")
        #     output_data[year_as_string] = []
        #
        # for area in sorted(dsr_groups):
        #     for act in sorted(dsr_groups[area]):
        #         for max_h in sorted(dsr_groups[area][act]):
        #             # static content without computations
        #             output_data[OutputDsrColumns.AREA] += [area]
        #             # output_data[OutputDsrColumns.NAME] += [f"DSR{max_h}"]
        #             output_data[OutputDsrColumns.GROUP] += ["DSR"]
        #
        #             for date_range in date_ranges:
        #                 data = dsr_groups[area][act][max_h].get(date_range, [])
        #                 output_data[date_range.strftime("%Y")] += [round(sum(data), 3)]
        #                 # output_data[OutputDsrColumns.NAME] += [f"DSR{i}"]

        return list(res.values())[0]

    def _build_filtered_dsr_cluster_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_dsr_cluster()
        df = filter_df_values_based_on_op_stat(self.op_stat_values, df)
        df = filter_non_declared_areas(self.main_params, df)
        df = filter_input_based_on_study_scenarios(df, self.main_params, self.years)
        df = filter_thermal_input_file_based_on_commission_date(df, self.years)
        df = filter_values_based_on_net_max_gen_cap(df)
        df = add_code_antares_colum(self.main_params, df)
        df = self._compute_dsr_cluster_years(df)
        return df

    def build_dsr_cluster(self) -> None:
        self._build_filtered_dsr_cluster_dataframe()
