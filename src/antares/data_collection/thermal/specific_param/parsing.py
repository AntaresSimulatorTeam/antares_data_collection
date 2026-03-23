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
from typing import Any

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    DEFAULT_DECOMMISSIONING_DATE,
    THERMAL_INPUT_FILE,
)
from antares.data_collection.thermal.specific_param.constants import (
    F_COLUMNS,
    P_COLUMNS,
    P_COLUMNS_WINTER,
    SPECIFIC_PARAM_FOLDER,
    InputThermalColumns,
    OutputThermalSpecificColumns,
    weighted_avg,
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
        scenario_name: str,
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.op_stat_values = op_stat_values
        self.main_params = main_params
        self.years = years
        self.scenario_name = scenario_name

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

    def _filter_non_declared_areas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Some nodes are not inside RTE study perimeter and therefore not registered inside the main parameters file.
        We don't want to consider them.
        We simply log a message for each area we find in this case
        """
        all_market_nodes = set(df[InputThermalColumns.MARKET_NODE])
        missing_nodes = []
        for node in all_market_nodes:
            antares_code = self.main_params.get_antares_code(node)
            if not antares_code:
                missing_nodes.append(node)
        return df[~df[InputThermalColumns.MARKET_NODE].isin(missing_nodes)]

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
        df[InputThermalColumns.COMMISSIONING_DATE] = pd.to_datetime(df[InputThermalColumns.COMMISSIONING_DATE])

        # Some values might be missing inside `DECOMMISSIONING_DATE_EXPECTED`.
        # If so, we should consider the decommissioning year to be 2100.
        df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] = pd.to_datetime(
            df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
        ).fillna(value=DEFAULT_DECOMMISSIONING_DATE)

        # # Some rows are exactly the same except for the capacity.
        # # We want to merge them by summing their capacity as in the end they'll be merged for PEGASE format
        # columns_to_group_on = df.columns.drop(InputThermalColumns.NET_MAX_GEN_CAP).tolist()
        # df = df.groupby(columns_to_group_on, as_index=False, dropna=False).sum()
        #
        # filtered_dfs = []
        # for commissioning_limits in self._get_starting_and_ending_timestamps():
        #     # For each year we only keep the values with fitting dates
        #     filtered_df = df.loc[
        #         (df[
        #              InputThermalColumns.COMMISSIONING_DATE] <= commissioning_limits.last_possible_commissioning_date)
        #         & (
        #                 df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
        #                 >= commissioning_limits.earliest_possible_decommissioning_date
        #         )
        #         ]
        #     filtered_dfs.append(filtered_df)
        #
        # # In the end we concatenate them and drop duplicated lines
        # df = pd.concat(filtered_dfs).drop_duplicates().reset_index(drop=True)

        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            msg = f"No input data matched the given (de)commissioning dates for the given years {self.years}"
            raise ValueError(msg)
        return df

    def _filter_values_based_on_net_max_gen_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """We do not consider clusters with a `NET_MAX_GEN_CAP` of 0."""
        return df.loc[df[InputThermalColumns.NET_MAX_GEN_CAP] > 0]

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

        # PLAN_OUTAGE_WINTER / PO_winter_default
        mask_plan_outage_annual_win = df[InputThermalColumns.PLAN_OUTAGE_WINTER].isna()
        if mask_plan_outage_annual_win.any():
            df = self._fill_from_common_data(
                df, mask_plan_outage_annual_win, InputThermalColumns.PLAN_OUTAGE_WINTER, "po_winter_default"
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
            InputThermalColumns.PLAN_OUTAGE_WINTER,
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

        years_date_format: dict[str, pd.Timestamp] = {
            str(year): pd.Timestamp(year=year, month=1, day=1) for year in years
        }

        df_computed = self._computations_thermal_specific(df, years_date_format)

        return df_computed

    def _computations_thermal_specific(
        self, df_to_compute: pd.DataFrame, years_input: dict[str, pd.Timestamp]
    ) -> pd.DataFrame:
        grouped_dfs = df_to_compute.groupby([ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN])

        output_data: dict[str, list[Any]] = {
            OutputThermalSpecificColumns.NODE: [],
            OutputThermalSpecificColumns.CLUSTER: [],
            OutputThermalSpecificColumns.NODE_ENTSOE: [],
            OutputThermalSpecificColumns.COMMENTS: [],
            OutputThermalSpecificColumns.CLUSTER_PEMMDB: [],
            "YEAR": [],
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
            **{col: [] for col in F_COLUMNS},
            **{col: [] for col in P_COLUMNS},
        }

        for (antares_node, cluster_name), grouped_df in grouped_dfs:
            for year_str, year_date in years_input.items():
                mask = (grouped_df[InputThermalColumns.COMMISSIONING_DATE] <= year_date) & (
                    grouped_df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] >= year_date
                )

                active_units = grouped_df.loc[mask]
                if active_units.empty:
                    continue

                # computations all indicators
                nb_unit = active_units.shape[0]

                cap = active_units[InputThermalColumns.NET_MAX_GEN_CAP]
                max_cap = cap.max()

                min_stable = active_units[InputThermalColumns.NET_MIN_STAB_GEN].sum() / max_cap

                efficiency = weighted_avg(
                    active_units, InputThermalColumns.STD_EFF_NCV, InputThermalColumns.NET_MAX_GEN_CAP
                )

                fo_rate = weighted_avg(
                    active_units, InputThermalColumns.FORCED_OUTAGE_RATE, InputThermalColumns.NET_MAX_GEN_CAP
                )

                fo_duration = weighted_avg(
                    active_units, InputThermalColumns.MEAN_TIME_REPAIR, InputThermalColumns.NET_MAX_GEN_CAP
                )

                po_duration = weighted_avg(
                    active_units, InputThermalColumns.PLAN_OUTAGE_ANNUAL_DAYS, InputThermalColumns.NET_MAX_GEN_CAP
                )

                po_winter = weighted_avg(
                    active_units, InputThermalColumns.PLAN_OUTAGE_WINTER, InputThermalColumns.NET_MAX_GEN_CAP
                )

                mr_specific = int(
                    (
                        active_units[
                            [
                                "GRP_MRUN_CURVE_ID",
                                "GEN_UNT_MRUN_CURVE_ID",
                                "GEN_UNT_INELASTIC_ID",
                            ]
                        ]
                        .notna()
                        .any()
                        .any()
                    )
                )

                cm_specific = int(
                    active_units[
                        [
                            "GEN_UNT_D_CURVE_ID",
                            "GRP_D_CURVE_ID",
                            "GEN_UNT_INELASTIC_ID",
                        ]
                    ]
                    .notna()
                    .any()
                    .any()
                )

                # ---- store result ----
                output_data[OutputThermalSpecificColumns.NODE].append(antares_node)
                output_data[OutputThermalSpecificColumns.CLUSTER].append(cluster_name)
                output_data[OutputThermalSpecificColumns.NODE_ENTSOE].append(pd.NA)
                output_data[OutputThermalSpecificColumns.COMMENTS].append(pd.NA)
                output_data[OutputThermalSpecificColumns.CLUSTER_PEMMDB].append(pd.NA)
                output_data["YEAR"].append(year_str)

                output_data[OutputThermalSpecificColumns.MIN_STABLE_GEN].append(min_stable)
                output_data[OutputThermalSpecificColumns.SPINNING].append(0)
                output_data[OutputThermalSpecificColumns.EFFICIENCY].append(efficiency)
                output_data[OutputThermalSpecificColumns.FO_RATE].append(fo_rate)
                output_data[OutputThermalSpecificColumns.FO_DURATION].append(fo_duration)
                output_data[OutputThermalSpecificColumns.PO_DURATION].append(po_duration)
                output_data[OutputThermalSpecificColumns.PO_WINTER].append(po_winter)
                output_data[OutputThermalSpecificColumns.MARGINAL_COST].append(pd.NA)
                output_data[OutputThermalSpecificColumns.MARKET_BID].append(pd.NA)
                output_data[OutputThermalSpecificColumns.MR_SPECIFIC].append(mr_specific)
                output_data[OutputThermalSpecificColumns.CM_SPECIFIC].append(cm_specific)
                output_data[OutputThermalSpecificColumns.NB_UNIT].append(nb_unit)
                for col in F_COLUMNS:
                    output_data[col].append(fo_rate)
                # winter
                for col in P_COLUMNS:
                    if col in P_COLUMNS_WINTER:
                        output_data[col].append((po_duration / 182) * po_winter)
                    else:
                        output_data[col].append((po_duration / 183) * (1 - po_winter))

        return pd.DataFrame(output_data)

    def _export_specific_param_dataframe(self, df: pd.DataFrame) -> None:
        parent_dir = self.output_folder / SPECIFIC_PARAM_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / f"specific_param_{self.scenario_name}.xlsx"

        with pd.ExcelWriter(output_path) as writer:
            for year, year_df in df.sort_values("YEAR").groupby("YEAR"):
                year_int = int(str(year))
                sheet_name = f"{year_int - 1}-{year_int}"

                year_df = year_df.sort_values(
                    by=[OutputThermalSpecificColumns.NODE, OutputThermalSpecificColumns.CLUSTER]
                ).drop(columns=["YEAR"])

                year_df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                )

    def build_specific_param(self) -> None:
        input_df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(input_df)
        df = self._filter_values_based_on_study_scenarios(df)

        df = self._filter_non_declared_areas(df)

        df = self._filter_values_based_on_commission_date(df)
        df = self._add_antares_cluster_name_colum(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)

        df = self._update_existing_columns_with_commondata(df)

        df = self._split_clusters_with_biomass_rule(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)

        df = self._add_code_antares_colum(df)
        df = self._filter_columns_for_output_specific(df)
        df = self._build_thermal_specific_pegase(df)
        self._export_specific_param_dataframe(df)
