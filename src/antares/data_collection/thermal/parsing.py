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

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pandas as pd

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    DEFAULT_DECOMMISSIONING_DATE,
    THERMAL_INPUT_FILE,
    InputThermalColumns,
)
from antares.data_collection.thermal.installed_power.parsing import ThermalInstallerPowerParser


@dataclass
class CommissioningDateLimits:
    last_possible_commissioning_date: pd.Timestamp
    earliest_possible_decommissioning_date: pd.Timestamp


class ThermalParser:
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
        self.filtered_dataframe = self._build_filtered_dataframe()

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

        if missing_nodes:
            return df[~df[InputThermalColumns.MARKET_NODE].isin(missing_nodes)]
        return df

    def _filter_non_declared_clusters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Some mapping between ENTSOE clusters and Antares ones might be missing in the `MainParams` file.
        If so, we do not want to crash but rather log that we'll not consider them.
        """
        all_pemmdb_clusters = set(df[InputThermalColumns.PEMMDB_TECHNOLOGY])
        missing_mappings = []
        for cluster_pemmdb in all_pemmdb_clusters:
            antares_cluster = self.main_params.get_cluster_bp(cluster_pemmdb)
            if not antares_cluster:
                missing_mappings.append(antares_cluster)

        if missing_mappings:
            return df[~df[InputThermalColumns.PEMMDB_TECHNOLOGY].isin(missing_mappings)]
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
        df[InputThermalColumns.COMMISSIONING_DATE] = pd.to_datetime(df[InputThermalColumns.COMMISSIONING_DATE])

        # Some values might be missing inside `DECOMMISSIONING_DATE_EXPECTED`.
        # If so, we should consider the decommissioning year to be 2100.
        df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED] = pd.to_datetime(
            df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
        ).fillna(value=DEFAULT_DECOMMISSIONING_DATE)

        # Reindex the dataframe to use Series freely
        df.index = pd.RangeIndex(len(df))

        commissioning_limits = list(self._get_starting_and_ending_timestamps())
        start_dates = df[InputThermalColumns.COMMISSIONING_DATE]
        end_dates = df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED]
        index_to_drop = []
        for k in range(len(df)):
            start_date = start_dates[k]
            end_date = end_dates[k]
            invalid_limits = 0
            for limit in commissioning_limits:
                if (
                    start_date > limit.last_possible_commissioning_date
                    or end_date < limit.earliest_possible_decommissioning_date
                ):
                    invalid_limits += 1

            # If no year matches the commissioning dates, we don't want to consider the row.
            if invalid_limits == len(commissioning_limits):
                index_to_drop.append(k)

        df = df.drop(index_to_drop)

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

    def _build_filtered_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(df)
        df = self._filter_non_declared_areas(df)
        df = self._filter_non_declared_clusters(df)
        df = self._filter_values_based_on_study_scenarios(df)
        df = self._filter_values_based_on_commission_date(df)
        df = self._add_antares_cluster_name_colum(df)
        df = self._split_clusters_with_biomass_rule(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)
        return self._add_code_antares_colum(df)

    def build_thermal_installed_power(self) -> None:
        parser = ThermalInstallerPowerParser(self.output_folder, self.main_params, self.years)
        parser.build_thermal_installed_power(self.filtered_dataframe)
