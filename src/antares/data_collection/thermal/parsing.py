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
    BIOMASS_CLUSTER_SUFFIX,
    BIOMASS_SNCD_FUEL_VALUE,
    THERMAL_INPUT_FILE,
    InputThermalColumns,
)
from antares.data_collection.thermal.installed_power.parsing import ThermalInstallerPowerParser
from antares.data_collection.thermal.param_modulation.parsing import ThermalParamModulationParser
from antares.data_collection.thermal.specific_param.parsing import ThermalSpecificParamParser
from antares.data_collection.thermal.utils import (
    add_antares_cluster_name_colum,
    parse_input_file,
)
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_df_input_file_based_on_commission_date,
    filter_df_values_based_on_op_stat,
    filter_input_based_on_study_scenarios,
    filter_non_declared_areas,
    filter_values_based_on_net_max_gen_cap,
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

    def _build_filtered_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file()
        df = filter_df_values_based_on_op_stat(self.op_stat_values, df, InputThermalColumns.OP_STAT.value)
        df = filter_non_declared_areas(self.main_params, df, InputThermalColumns.MARKET_NODE.value)
        df = self._filter_non_declared_clusters(df)
        df = filter_input_based_on_study_scenarios(
            df, self.main_params, self.years, InputThermalColumns.STUDY_SCENARIO.value
        )
        df = filter_df_input_file_based_on_commission_date(
            df,
            self.years,
            InputThermalColumns.COMMISSIONING_DATE.value,
            InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        )
        df = add_antares_cluster_name_colum(self.main_params, df)
        df = self._split_clusters_with_biomass_rule(df)
        df = filter_values_based_on_net_max_gen_cap(df, InputThermalColumns.NET_MAX_GEN_CAP.value)
        return add_code_antares_colum(self.main_params, df)

    def build_installed_power(self) -> None:
        parser = ThermalInstallerPowerParser(self.output_folder, self.main_params, self.years)
        parser.build_thermal_installed_power(self.filtered_dataframe)

    def build_param_modulation(self) -> None:
        parser = ThermalParamModulationParser(self.input_folder, self.output_folder, self.main_params, self.years)
        parser.build_param_modulation(self.filtered_dataframe)

    def build_specific_param(self) -> None:
        parser = ThermalSpecificParamParser(self.output_folder, self.main_params, self.years)
        parser.build_thermal_specific_param(self.filtered_dataframe)
