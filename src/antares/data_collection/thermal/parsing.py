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

from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    THERMAL_INPUT_FILE,
    InputThermalColumns,
)
from antares.data_collection.thermal.installed_power.parsing import ThermalInstallerPowerParser
from antares.data_collection.thermal.param_modulation.parsing import ThermalSpecificParametersParser
from antares.data_collection.thermal.specific_param.parsing import ThermalSpecificParamParser
from antares.data_collection.thermal.utils import (
    filter_input_based_on_study_scenarios,
    filter_thermal_input_file_based_on_commission_date,
    parse_input_file,
)


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
        return parse_input_file(self.input_folder.joinpath(THERMAL_INPUT_FILE), list(InputThermalColumns))

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

    def _build_filtered_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(df)
        df = self._filter_non_declared_areas(df)
        df = self._filter_non_declared_clusters(df)
        df = filter_input_based_on_study_scenarios(df, self.main_params, self.years)
        df = filter_thermal_input_file_based_on_commission_date(df, self.years)
        df = self._add_antares_cluster_name_colum(df)
        df = self._split_clusters_with_biomass_rule(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)
        return self._add_code_antares_colum(df)

    def build_installed_power(self) -> None:
        parser = ThermalInstallerPowerParser(self.output_folder, self.main_params, self.years)
        parser.build_thermal_installed_power(self.filtered_dataframe)

    def build_specific_parameters(self) -> None:
        parser = ThermalSpecificParametersParser(self.input_folder, self.output_folder, self.main_params, self.years)
        parser.build_thermal_specific_parameters(self.filtered_dataframe)

    def build_specific_param(self) -> None:
        parser = ThermalSpecificParamParser(self.output_folder, self.main_params, self.years)
        parser.build_thermal_specific_param(self.filtered_dataframe)
