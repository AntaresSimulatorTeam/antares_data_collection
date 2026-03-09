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
from typing import Any, Iterator

import pandas as pd

from pandas import DataFrame

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constant_specific import InputThermalColumns, OutputThermalSpecificColumns
from antares.data_collection.thermal.constants import (
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    DEFAULT_DECOMMISSIONING_DATE,
    FUEL_MAPPING,
    THERMAL_INPUT_FILE,
    get_starting_and_ending_timestamps_for_outputs,
)
from antares.data_collection.thermal.parsing.installed_power import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    CommissioningDateLimits,
)


# TODO most of method are similar to installed power parser, we should refactor them
class ThermalSpecificParamParser:
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

    def _read_input_file(self) -> pd.DataFrame:
        input_file_path = self.input_folder.joinpath(THERMAL_INPUT_FILE)
        if not input_file_path.exists():
            raise ValueError(f"Thermal input file {input_file_path} not found")

        # Checks that all expected columns exist
        df = pd.read_csv(input_file_path)
        existing_cols = set(df.columns)
        expected_cols = list(InputThermalColumns)
        for expected_column in expected_cols:
            if expected_column not in existing_cols:
                raise ValueError(f"Column {expected_column} not found in {input_file_path}")

        # Return the dataframe with the useful columns only
        return df[expected_cols]

    def _filter_values_based_on_op_stat(self, df: pd.DataFrame) -> pd.DataFrame:
        """We want to keep only the lines were the OP_STAT value matches the user given ones"""
        if not self.op_stat_values:
            return df
        df = df[df[InputThermalColumns.OP_STAT].isin(self.op_stat_values)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"The given op_stat values {self.op_stat_values} are not present in the dataframe")
        return df

    def _filter_values_based_on_study_scenarios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Using MainParams and the user given years, we retrieve the study scenarios we have to consider.
        Other scenarios present in the input file will be ignored.
        """
        scenario_types = list(self.main_params.get_scenario_types(years=self.years))

        if not scenario_types:
            return df

        df = df[df[InputThermalColumns.STUDY_SCENARIO].str.contains("|".join(scenario_types), case=False, na=False)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"No input data matched the given study scenario for the given years {self.years}")
        return df

    def _filter_values_based_on_commission_date(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.years:
            return df

        # Dates objects are stored as Strings for the moment, we have to change this to perform checks.
        for datetime_col in [InputThermalColumns.COMMISSIONING_DATE, InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]:
            df[datetime_col] = pd.to_datetime(df[datetime_col])

        # Dates objects are stored as Strings for the moment, we have to change this to perform checks.
        df[InputThermalColumns.COMMISSIONING_DATE] = pd.to_datetime(df[InputThermalColumns.COMMISSIONING_DATE])
        # Some values might be missing inside `DECOMMISSIONING_DATE_EXPECTED`.
        # If so, we should consider the decommissioning year to be 2100.
        df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] = pd.to_datetime(
            df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
        ).fillna(value=DEFAULT_DECOMMISSIONING_DATE)

        filtered_dfs = []
        for commissioning_limits in self._get_starting_and_ending_timestamps():
            # For each year we only keep the values with fitting dates
            filtered_df = df.loc[
                (df[InputThermalColumns.COMMISSIONING_DATE] <= commissioning_limits.last_possible_commissioning_date)
                & (
                    df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
                    >= commissioning_limits.earliest_possible_decommissioning_date
                )
            ]
            filtered_dfs.append(filtered_df)

        # In the end we concatenate them and drop duplicated lines
        df = pd.concat(filtered_dfs).drop_duplicates().reset_index(drop=True)

        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            msg = f"No input data matched the given (de)commissioning dates for the given years {self.years}"
            raise ValueError(msg)
        return df

    def _filter_values_based_on_net_max_gen_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """We do not consider clusters with a `NET_MAX_GEN_CAP` of 0."""
        return df.loc[df[InputThermalColumns.NET_MAX_GEN_CAP] > 0]

    def _get_starting_and_ending_timestamps(self) -> Iterator[CommissioningDateLimits]:
        """
        For each year in `self.years`, we should consider:
        - 31st December of the year -> Each cluster with a commissioning date after this will not be considered.
        - 1st January of previous year -> Each cluster with a decommissioning date before this will not be considered.
        """
        for year in self.years:
            yield CommissioningDateLimits(
                last_possible_commissioning_date=pd.Timestamp(year=year, month=12, day=31),
                earliest_possible_decommissioning_date=pd.Timestamp(year=year - 1, month=1, day=1),
            )

    def _add_antares_cluster_name_colum(self, df: pd.DataFrame) -> pd.DataFrame:
        cluster_list = df[InputThermalColumns.PEMMDB_TECHNOLOGY].tolist()
        df[ANTARES_CLUSTER_NAME_COLUMN] = self.main_params.get_clusters_bp(cluster_list)
        return df

    def _split_clusters_with_biomass_rule(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        If the column `SNCD_FUEL` is set to `Bio`, we have to split the PEMMDB Cluster into 2 Antares ones.
        We split its capacity based on its `SNCD_FUEL_RT` value.
        """
        # Create a boolean mask for biomass rows
        biomass_mask = df[InputThermalColumns.SCND_FUEL] == BIOMASS_SNCD_FUEL_VALUE

        # Get the biomass rows
        biomass_rows = df[biomass_mask].copy()

        # Create new biomass lines
        biomass_rows[ANTARES_CLUSTER_NAME_COLUMN] += f" {BIOMASS_CLUSTER_SUFFIX}"
        biomass_rows[InputThermalColumns.NET_MAX_GEN_CAP] *= biomass_rows[InputThermalColumns.SCND_FUEL_RT]

        # Update the original rows
        df.loc[biomass_mask, InputThermalColumns.NET_MAX_GEN_CAP] *= (
                1 - df.loc[biomass_mask, InputThermalColumns.SCND_FUEL_RT]
        )

        # Concatenate the original and new biomass rows
        df = pd.concat([df, biomass_rows], ignore_index=True)

        return df

    def _add_code_antares_colum(self, df: pd.DataFrame) -> pd.DataFrame:
        node_list = df[InputThermalColumns.MARKET_NODE].tolist()
        df[ANTARES_NODE_NAME_COLUMN] = self.main_params.get_antares_codes(node_list)
        return df

    def _update_existing_columns_with_commondata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Use Common Data if empty or missing values in the input file for columns define in class `InputThermalColumns`.

        Additional rules:
            - STD_EFF_NCV: if value > 1 then apply div by 100
            - NET_MIN_STAB_GEN: min_stable_generation_default*NET_MAX_GEN_CAP
        """

        # STD_EFF_NCV / efficiency_default
        # TODO add to MAINPARAM parser to check `efficiency_default`ranged value [0;1]
        mask_std_eff_nvc = df[InputThermalColumns.STD_EFF_NCV].isna()
        if mask_std_eff_nvc.any():
            df = self._fill_from_common_data(
                df, mask_std_eff_nvc, InputThermalColumns.STD_EFF_NCV, "efficiency_default"
            )

        # FORCED_OUTAGE_RATE / FO_rate_default
        mask_forced_outage_rate = df[InputThermalColumns.FORCED_OUTAGE_RATE].isna()
        if mask_forced_outage_rate.any():
            df = self._fill_from_common_data(
                df, mask_forced_outage_rate, InputThermalColumns.FORCED_OUTAGE_RATE, "fo_rate_default"
            )

        # MEAN_TIME_REPAIR / FO_duration_default
        mask_mean_time_repair = df[InputThermalColumns.MEAN_TIME_REPAIR].isna()
        if mask_mean_time_repair.any():
            df = self._fill_from_common_data(
                df, mask_mean_time_repair, InputThermalColumns.MEAN_TIME_REPAIR, "fo_duration_default"
            )

        # PLAN_OUTAGE_ANNUAL_DAYS / PO_duration_default
        mask_plan_outage_annual_days = df[InputThermalColumns.PLAN_OUTAGE_ANNUAL_DAYS].isna()
        if mask_plan_outage_annual_days.any():
            df = self._fill_from_common_data(
                df, mask_plan_outage_annual_days, InputThermalColumns.PLAN_OUTAGE_ANNUAL_DAYS, "po_duration_default"
            )

        # PLAN_OUTAGE_ANNUAL_WIN / PO_winter_default
        mask_plan_outage_annual_win = df[InputThermalColumns.PLAN_OUTAGE_ANNUAL_WIN].isna()
        if mask_plan_outage_annual_win.any():
            df = self._fill_from_common_data(
                df, mask_plan_outage_annual_win, InputThermalColumns.PLAN_OUTAGE_ANNUAL_WIN, "po_winter_default"
            )

        # NET_MIN_STAB_GEN / min_stable_generation_default
        mask_net_min_stab_gen = df[InputThermalColumns.NET_MIN_STAB_GEN].isna()
        if mask_net_min_stab_gen.any():
            df = self._fill_from_common_data(
                df, mask_net_min_stab_gen, InputThermalColumns.NET_MIN_STAB_GEN, "min_stable_generation_default"
            )
            # specific treatment
            df.loc[mask_net_min_stab_gen, InputThermalColumns.NET_MIN_STAB_GEN] *= df[
                InputThermalColumns.NET_MAX_GEN_CAP
            ]

        return df

    def _fill_from_common_data(
        self, dftofill: pd.DataFrame, mask: pd.Series, column: InputThermalColumns, attr: str
    ) -> pd.DataFrame:
        df_to_fill = dftofill.copy()
        clusters = df_to_fill.loc[mask, ANTARES_CLUSTER_NAME_COLUMN].tolist()
        values = [getattr(x, attr) for x in self.main_params.get_antares_clusters_technology_and_fuel(clusters)]
        df_to_fill.loc[mask, column] = values

        return df_to_fill

    def _filter_columns_for_output_specific(self, df: pd.DataFrame) -> pd.DataFrame:
        """Only keep the input columns we need to create the output file."""
        expected_cols = [
            InputThermalColumns.COMMISSIONING_DATE,
            InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED,
            ANTARES_CLUSTER_NAME_COLUMN,
            ANTARES_NODE_NAME_COLUMN,
            InputThermalColumns.NET_MAX_GEN_CAP,
            InputThermalColumns.STD_EFF_NCV,
            InputThermalColumns.FORCED_OUTAGE_RATE,
            InputThermalColumns.MEAN_TIME_REPAIR,
            InputThermalColumns.PLAN_OUTAGE_ANNUAL_DAYS,
            InputThermalColumns.PLAN_OUTAGE_ANNUAL_WIN,
            InputThermalColumns.NET_MIN_STAB_GEN,
            InputThermalColumns.GRP_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_MRUN_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
            InputThermalColumns.GEN_UNT_D_CURVE_ID,
            InputThermalColumns.GRP_D_CURVE_ID,
            InputThermalColumns.GEN_UNT_INELASTIC_ID,
        ]
        return df[expected_cols]

    def _build_thermal_specific_pegase(self, df: pd.DataFrame) -> pd.DataFrame:
        years = self.years

        grouped_dfs = df.groupby([ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN])
        output_data: dict[str, list[Any]] = {
            OutputThermalSpecificColumns.NODE: [],
            OutputThermalSpecificColumns.CLUSTER: [],
            OutputThermalSpecificColumns.MIN_STABLE_GEN: [],
            OutputThermalSpecificColumns.SPINNING: [],
            OutputThermalSpecificColumns.EFFICIENCY: [],
            OutputThermalSpecificColumns.FO_RATE: [],
            OutputThermalSpecificColumns.FO_DURATION: [],
            OutputThermalSpecificColumns.PO_DURATION: [],
            OutputThermalSpecificColumns.PO_WINTER: [],
            OutputThermalSpecificColumns.MARGINAL_COST: [],
            OutputThermalSpecificColumns.MARKET_BID: [],
            OutputThermalSpecificColumns.MR_SPECIFIC: [],
            OutputThermalSpecificColumns.CM_SPECIFIC: [],
            OutputThermalSpecificColumns.NB_UNIT: [],
          }

        # TODO just filter with year (eg 2030, 2030-01-01)
        for date_range in date_ranges:
            for month in date_range:
                month_as_string = month.strftime("%Y_%m")
                output_data[month_as_string] = []

        for (antares_node, cluster_name), grouped_df in grouped_dfs:
            assert isinstance(cluster_name, str)
            # We have to handle `Bio` clusters as we don't have their mapping inside the `MainParams` class
            unit_name = cluster_name.removesuffix(f" {BIOMASS_CLUSTER_SUFFIX}")

            # technology = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).technology
            # fuel = self._find_fuel(unit_name)
            # efficiency_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).efficiency_default
            # fo_rate_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).fo_rate_default
            # fo_duration_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).fo_duration_default
            # po_duration_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).po_duration_default
            # po_winter_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).po_winter_default
            # min_stable_generation_default = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).min_stable_generation_default


            output_data[OutputThermalSpecificColumns.NODE] += [antares_node]
            output_data[OutputThermalSpecificColumns.CLUSTER] += [cluster_name]

            # TODO process function to compute specific params
            output_data[OutputThermalSpecificColumns.MIN_STABLE_GEN] += [] # sum(NET_MIN_STAB_GEN)/max(NET_MAX_GEN_CAP)
            output_data[OutputThermalSpecificColumns.SPINNING] += [0]
            output_data[OutputThermalSpecificColumns.EFFICIENCY] += [] # STD_EFF_NCV pondarate by NET_MAX_GEN_CAP
            output_data[OutputThermalSpecificColumns.FO_RATE] += [] # FORCED_OUTAGE_RATE pondarate by NET_MAX_GEN_CAP
            output_data[OutputThermalSpecificColumns.FO_DURATION] += [] # MEAN_TIME_REPAIR pondarate by NET_MAX_GEN_CAP
            output_data[OutputThermalSpecificColumns.PO_DURATION] += [] # PLAN_OUTAGE_ANNUAL_DAYS pondarate by NET_MAX_GEN_CAP
            output_data[OutputThermalSpecificColumns.PO_WINTER] += [] # PLAN_OUTAGE_ANNUAL_WIN pondarate by NET_MAX_GEN_CAP
            output_data[OutputThermalSpecificColumns.MARGINAL_COST] += [] # empty
            output_data[OutputThermalSpecificColumns.MARKET_BID] += [] # empty
            output_data[OutputThermalSpecificColumns.MR_SPECIFIC] += [] # at least one unit in those columns GRP_MRUN_CURVE_ID, GEN_UNT_MRUN_CURVE_ID , GEN_UNT_INELASTIC_ID THEN 1 ELSE 0

            for date_range in date_ranges:
                for month in date_range:
                    month_end = month + pd.offsets.MonthEnd(1)
                    # Find rows where the range overlaps with the current month
                    mask = (grouped_df[InputThermalColumns.COMMISSIONING_DATE] <= month_end) & (
                        grouped_df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] >= month
                    )
                    data = grouped_df.loc[mask, InputThermalColumns.NET_MAX_GEN_CAP]
                    output_data[month.strftime("%Y_%m")] += [data.sum(), data.count()]


        return df

    def build_specific_param(self) -> DataFrame:
        input_df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(input_df)
        df = self._filter_values_based_on_study_scenarios(df)
        df = self._filter_values_based_on_commission_date(df)

        df = self._add_antares_cluster_name_colum(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)
        df = self._update_existing_columns_with_commondata(df)

        df = self._split_clusters_with_biomass_rule(df)

        df = self._add_code_antares_colum(df)
        df = self._filter_columns_for_output_specific(df)
        df = self._build_thermal_specific_pegase(df)

        # TODO add method to check and update columns with values from Common Data
        # TODO add method to filter columns only needed
        # TODO build pegase dataframe

        return df
