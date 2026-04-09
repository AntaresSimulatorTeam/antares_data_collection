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
    InputThermalColumns,
)
from antares.data_collection.thermal.param_modulation.constants import (
    CAPACITY_MODULATION_NAME,
    TECHNICAL_PARAMS_FOLDER,
    OutputHoursColumns,
)
from antares.data_collection.thermal.specific_param.constants import (
    F_COLUMNS,
    P_COLUMNS,
    P_COLUMNS_WINTER,
    SPECIFIC_PARAM_FOLDER,
    SPECIFIC_PARAM_NAME_FILE,
    TAG_YEAR_COL,
    OutputThermalSpecificColumns,
    weighted_avg,
)
from antares.data_collection.thermal.utils import apply_round_to_numeric_columns


class ThermalSpecificParamParser:
    def __init__(self, output_folder: Path, main_params: MainParams, years: list[int]):
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _update_existing_columns_with_commondata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Use Common Data if empty or missing values in the input file for columns define in class `InputThermalColumns`.

        Additional rules:
            - NET_MIN_STAB_GEN: min_stable_generation_default*NET_MAX_GEN_CAP
        """

        # STD_EFF_NCV / efficiency_default
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
        mask_net_min_stab_gen = (df[InputThermalColumns.NET_MIN_STAB_GEN].isna()) | (
            df[InputThermalColumns.NET_MIN_STAB_GEN] == 0
        )
        if mask_net_min_stab_gen.any():
            df = self._fill_from_common_data(
                df, mask_net_min_stab_gen, InputThermalColumns.NET_MIN_STAB_GEN, "min_stable_generation_default"
            )
            # specific treatment
            df.loc[mask_net_min_stab_gen, InputThermalColumns.NET_MIN_STAB_GEN] *= df[
                InputThermalColumns.NET_MAX_GEN_CAP
            ]

        return df

    def _update_column_net_min_stab_gen(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Update NET_MIN_STAB_GEN for biomass rows when SCND_FUEL_RT is in ]0;1[.
            - If cluster name contains "bio" → multiply by SCND_FUEL_RT
            - Otherwise → multiply by (1 - SCND_FUEL_RT)
        """

        # Mask: SCND_FUEL_RT in ]0;1[
        scnd_fuel_rt_mask = (df[InputThermalColumns.SCND_FUEL_RT] > 0) & (df[InputThermalColumns.SCND_FUEL_RT] < 1)

        # Mask: biomass rows
        biomass_mask = (df[InputThermalColumns.SCND_FUEL] == BIOMASS_SNCD_FUEL_VALUE) & scnd_fuel_rt_mask

        # Mask: cluster name contains BIOMASS_CLUSTER_SUFFIX
        bio_name_mask = df[ANTARES_CLUSTER_NAME_COLUMN].str.contains(BIOMASS_CLUSTER_SUFFIX, case=False, na=False)

        # Final masks
        biomass_bio_mask = biomass_mask & bio_name_mask
        biomass_non_bio_mask = biomass_mask & ~bio_name_mask

        # Update "bio" rows
        df.loc[biomass_bio_mask, InputThermalColumns.NET_MIN_STAB_GEN] *= df.loc[
            biomass_bio_mask, InputThermalColumns.SCND_FUEL_RT
        ]

        # Update "non-bio" rows
        df.loc[biomass_non_bio_mask, InputThermalColumns.NET_MIN_STAB_GEN] *= (
            1 - df.loc[biomass_non_bio_mask, InputThermalColumns.SCND_FUEL_RT]
        )

        return df

    def _fill_from_common_data(
        self, dftofill: pd.DataFrame, mask: pd.Series, column: InputThermalColumns, attr: str
    ) -> pd.DataFrame:
        df_to_fill = dftofill.copy()
        clusters = df_to_fill.loc[mask, ANTARES_CLUSTER_NAME_COLUMN].tolist()

        # We have to handle `Bio` clusters as we don't have their mapping inside the `MainParams` class
        clusters = [cluster.removesuffix(f" {BIOMASS_CLUSTER_SUFFIX}") for cluster in clusters]

        values = [getattr(x, attr) for x in self.main_params.get_antares_clusters_common_data_params(clusters)]
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

    def _build_thermal_specific_pegase(self, df: pd.DataFrame, df_cm_min_values: pd.DataFrame) -> pd.DataFrame:
        years = self.years

        years_date_format: dict[str, pd.Timestamp] = {
            str(year): pd.Timestamp(year=year, month=1, day=1) for year in years
        }

        df_computed = self._computations_thermal_specific(df, years_date_format, df_cm_min_values)

        return df_computed

    def _computations_thermal_specific(
        self, df_to_compute: pd.DataFrame, years_input: dict[str, pd.Timestamp], df_cm_min_values: pd.DataFrame
    ) -> pd.DataFrame:
        grouped_dfs = df_to_compute.groupby([ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN])

        output_data: dict[str, list[Any]] = {
            OutputThermalSpecificColumns.NODE: [],
            OutputThermalSpecificColumns.CLUSTER: [],
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

                ##
                # special treatments for `min_stable_gen`
                ##

                # need to compare and keep min value with min of TS from capacity modulation file
                min_stable = active_units[InputThermalColumns.NET_MIN_STAB_GEN].sum() / cap.sum()

                # get min of TS capacity modulation
                name_col_target = f"{antares_node}_{cluster_name}"

                if name_col_target in df_cm_min_values.columns:
                    cm_min_value = df_cm_min_values.loc[
                        df_cm_min_values[TAG_YEAR_COL] == year_str, name_col_target
                    ].dropna()

                    if not cm_min_value.empty:
                        min_stable = min(min_stable, cm_min_value.iloc[0])

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

        output_path = parent_dir / SPECIFIC_PARAM_NAME_FILE

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

    def _check_capacity_modulation_files_exist(self) -> None:
        """Check if the capacity modulation files exist."""
        years = self.years

        for year in years:
            name_file = f"{CAPACITY_MODULATION_NAME}_{year - 1}-{year}.csv"
            capacity_modulation_file = self.output_folder / TECHNICAL_PARAMS_FOLDER / name_file
            if not capacity_modulation_file.exists():
                raise FileNotFoundError(
                    f"Capacity modulation file not found to compute minimal values of time series: {capacity_modulation_file}"
                )

    def _parse_capacity_modulation_file_and_compute_ts_min_values(self) -> pd.DataFrame:
        """Parse the capacity modulation file."""
        years = self.years

        result = []
        for year in years:
            name_file = f"{CAPACITY_MODULATION_NAME}_{year - 1}-{year}.csv"
            capacity_modulation_file = self.output_folder / TECHNICAL_PARAMS_FOLDER / name_file

            # .copy() used to avoid pytest warning ("PerformanceWarning")
            df_year = pd.read_csv(capacity_modulation_file).copy()

            # cast to "str" to be compatible with dict[str, pd.Timestamp] then
            df_year[TAG_YEAR_COL] = str(year)

            result.append(df_year)

        df_concat = pd.concat(result, ignore_index=True)

        # compute min of TS param modulation
        exclude_columns = [OutputHoursColumns.HOUR, OutputHoursColumns.DATE]
        names_to_keep = set(df_concat.columns) - set(exclude_columns)
        df_concat = df_concat[sorted(list(names_to_keep))]

        df_min_values = df_concat.groupby(TAG_YEAR_COL, as_index=False).min()

        return df_min_values

    def build_thermal_specific_param(self, df: pd.DataFrame) -> None:
        df = self._update_existing_columns_with_commondata(df)
        df = self._update_column_net_min_stab_gen(df)
        self._check_capacity_modulation_files_exist()

        # read files + compute min of TS param modulation and return df
        df_capacity_modulation_min_values = self._parse_capacity_modulation_file_and_compute_ts_min_values()

        df = self._filter_columns_for_output_specific(df)
        df = self._build_thermal_specific_pegase(df, df_capacity_modulation_min_values)
        df = apply_round_to_numeric_columns(
            df, [OutputThermalSpecificColumns.FO_DURATION, OutputThermalSpecificColumns.PO_DURATION]
        )
        self._export_specific_param_dataframe(df)
