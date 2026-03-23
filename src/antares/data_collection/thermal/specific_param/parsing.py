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
    InputThermalColumns,
)
from antares.data_collection.thermal.specific_param.constants import (
    F_COLUMNS,
    P_COLUMNS,
    P_COLUMNS_WINTER,
    SPECIFIC_PARAM_FOLDER,
    OutputThermalSpecificColumns,
    weighted_avg,
)


# TODO most of method are similar to installed power parser, we should refactor them
class ThermalSpecificParamParser:
    def __init__(
        self,
        output_folder: Path,
        main_params: MainParams,
        years: list[int],
        scenario_name: str,
    ):
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years
        self.scenario_name = scenario_name

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

    def build_thermal_specific_param(self, df: pd.DataFrame) -> None:
        # df = self._filter_values_based_on_op_stat(input_df)
        # df = self._filter_values_based_on_study_scenarios(df)
        #
        # df = self._filter_non_declared_areas(df)
        #
        # df = self._filter_values_based_on_commission_date(df)
        # df = self._add_antares_cluster_name_colum(df)
        # df = self._filter_values_based_on_net_max_gen_cap(df)

        df = self._update_existing_columns_with_commondata(df)

        # df = self._split_clusters_with_biomass_rule(df)
        # df = self._filter_values_based_on_net_max_gen_cap(df)
        #
        # df = self._add_code_antares_colum(df)

        df = self._filter_columns_for_output_specific(df)
        df = self._build_thermal_specific_pegase(df)
        self._export_specific_param_dataframe(df)
