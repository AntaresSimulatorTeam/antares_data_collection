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
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    DEFAULT_DECOMMISSIONING_DATE,
    FUEL_MAPPING,
    THERMAL_INPUT_FILE,
    THERMAL_INSTALL_POWER_FOLDER,
    InputThermalColumns,
    OutputThermalInstallPowerColumns,
    get_starting_and_ending_timestamps_for_outputs,
)

ANTARES_CLUSTER_NAME_COLUMN = "cluster_name"
ANTARES_NODE_NAME_COLUMN = "antares_node"


class ThermalInstallerPowerParser:
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

        if len(scenario_types) == 2:
            # The input writing is `X&Y` or `Y&X` so we have to consider that
            scenario_types.append(f"{scenario_types[0]}&{scenario_types[1]}")
            scenario_types.append(f"{scenario_types[1]}&{scenario_types[0]}")

        df = df[df[InputThermalColumns.STUDY_SCENARIO].isin(scenario_types)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"No input data matched the given study scenario for the given years {self.years}")
        return df

    def _filter_values_based_on_commission_date(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.years:
            return df

        start, end = self._get_starting_and_ending_timestamps()

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

        df = df.loc[
            (df[InputThermalColumns.COMMISSIONING_DATE] <= start)
            & (df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] >= end)
        ]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            msg = f"No input data matched the given (de)commissioning dates for the given years {self.years}"
            raise ValueError(msg)
        return df

    def _filter_values_based_on_net_max_gen_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """We do not consider clusters with a `NET_MAX_GEN_CAP` of 0."""
        return df.loc[df[InputThermalColumns.NET_MAX_GEN_CAP] > 0]

    def _get_starting_and_ending_timestamps(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        years = sorted(self.years)
        start = pd.Timestamp(year=years[0] - 1, month=1, day=1)
        end = pd.Timestamp(year=years[-1], month=12, day=31)
        return start, end

    def _add_antares_cluster_name_colum(self, df: pd.DataFrame) -> pd.DataFrame:
        cluster_list = df[InputThermalColumns.PEMMDB_TECHNOLOGY].tolist()
        df[ANTARES_CLUSTER_NAME_COLUMN] = self.main_params.get_clusters_bp(cluster_list)
        return df

    def _split_clusters_with_biomass_rule(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        If the column `SNCD_FUEL` is set to `Bio`, we have to split the PEMMDB Cluster into 2 Antares ones.
        We split its capacity based on its `SNCD_FUEL_RT` value.
        """
        fuels = df[InputThermalColumns.SCND_FUEL]
        for k, fuel in enumerate(fuels):
            if fuel == BIOMASS_SNCD_FUEL_VALUE:
                cluster_line = df.iloc[k]

                # Add new line inside dataframe with the created biomass unit
                bio_line = deepcopy(cluster_line)
                bio_line[ANTARES_CLUSTER_NAME_COLUMN] += f" {BIOMASS_CLUSTER_SUFFIX}"
                bio_line[InputThermalColumns.NET_MAX_GEN_CAP] *= bio_line[InputThermalColumns.SCND_FUEL_RT]
                df.loc[len(df)] = bio_line

                # Replace fuel cluster with new `NET_MAX_GEN_CAP` value
                cluster_line[InputThermalColumns.NET_MAX_GEN_CAP] *= 1 - cluster_line[InputThermalColumns.SCND_FUEL_RT]
                df.iloc[k] = cluster_line

        return df

    def _add_code_antares_colum(self, df: pd.DataFrame) -> pd.DataFrame:
        node_list = df[InputThermalColumns.MARKET_NODE].tolist()
        df[ANTARES_NODE_NAME_COLUMN] = self.main_params.get_antares_codes(node_list)
        return df

    def _filter_columns_for_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """Only keep the input columns we need to create the output file."""
        expected_cols = [
            InputThermalColumns.COMMISSIONING_DATE,
            InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED,
            ANTARES_CLUSTER_NAME_COLUMN,
            ANTARES_NODE_NAME_COLUMN,
            InputThermalColumns.PEMMDB_TECHNOLOGY,
            InputThermalColumns.NET_MAX_GEN_CAP,
        ]
        return df[expected_cols]

    def _get_start_and_end_timestamps_for_outputs(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        years = sorted(self.years)
        start, _ = get_starting_and_ending_timestamps_for_outputs(years[0])
        _, end = get_starting_and_ending_timestamps_for_outputs(years[-1])
        return start, end

    def _find_fuel(self, unit_name: str) -> str:
        for pattern, value in FUEL_MAPPING.items():
            if pattern in unit_name:
                return value
        return self.main_params.get_antares_cluster_technology_and_fuel(unit_name).fuel

    def _build_pegase_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        start, end = self._get_start_and_end_timestamps_for_outputs()
        # Create a date range from min start to max end, monthly frequency
        date_range = pd.date_range(start=start, end=end, freq="MS")  # MS = Month Start

        grouped_dfs = df.groupby([ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN])
        output_data: dict[str, list[Any]] = {
            OutputThermalInstallPowerColumns.AREA: [],
            OutputThermalInstallPowerColumns.FUEL: [],
            OutputThermalInstallPowerColumns.TECHNOLOGY: [],
            OutputThermalInstallPowerColumns.CLUSTER: [],
            OutputThermalInstallPowerColumns.CATEGORY: [],
        }
        for month in date_range:
            output_data[month.strftime("%Y_%m")] = []

        for (antares_node, cluster_name), grouped_df in grouped_dfs:
            assert isinstance(cluster_name, str)
            # We have to handle `Bio` clusters as we don't have their mapping inside the `MainParams` class
            unit_name = cluster_name.removesuffix(f" {BIOMASS_CLUSTER_SUFFIX}")
            technology = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).technology
            fuel = self._find_fuel(unit_name)

            output_data[OutputThermalInstallPowerColumns.AREA] += 2 * [antares_node]
            output_data[OutputThermalInstallPowerColumns.FUEL] += 2 * [fuel]
            output_data[OutputThermalInstallPowerColumns.TECHNOLOGY] += 2 * [technology]
            output_data[OutputThermalInstallPowerColumns.CLUSTER] += 2 * [cluster_name]
            output_data[OutputThermalInstallPowerColumns.CATEGORY] += ["number", "power"]

            for month in date_range:
                month_end = month + pd.offsets.MonthEnd(1)
                # Find rows where the range overlaps with the current month
                mask = (grouped_df[InputThermalColumns.COMMISSIONING_DATE] <= month_end) & (
                    grouped_df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] >= month
                )
                data = grouped_df.loc[mask, InputThermalColumns.NET_MAX_GEN_CAP]
                output_data[month.strftime("%Y_%m")] += [data.sum(), data.count()]

        # Add the `ToUse` column with every value being a 1
        dataframe = pd.DataFrame(output_data)
        to_use_col = OutputThermalInstallPowerColumns.TO_USE
        dataframe[to_use_col] = 1

        # Reorder the dataframe columns (just need to put `ToUse` in first)
        return dataframe[[to_use_col] + [col for col in dataframe.columns if col != to_use_col]]

    def _export_dataframe(self, df: pd.DataFrame) -> None:
        parent_dir = self.output_folder / THERMAL_INSTALL_POWER_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)
        df.to_excel(parent_dir / "thermal_installed_power.xlsx", index=False)

    def build_thermal_installed_power(self) -> None:
        input_df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(input_df)
        df = self._filter_values_based_on_study_scenarios(df)
        df = self._filter_values_based_on_commission_date(df)
        df = self._add_antares_cluster_name_colum(df)
        df = self._split_clusters_with_biomass_rule(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)
        df = self._add_code_antares_colum(df)
        df = self._filter_columns_for_output(df)
        df = self._build_pegase_dataframe(df)
        self._export_dataframe(df)
