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

from antares.data_collection.constants import MAX_DECIMAL_DIGITS
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.constants import (
    ANTARES_CLUSTER_NAME_COLUMN,
    ANTARES_NODE_NAME_COLUMN,
    BIOMASS_CLUSTER_SUFFIX,
    FUEL_MAPPING,
    InputThermalColumns,
    get_starting_and_ending_timestamps_for_outputs,
)
from antares.data_collection.thermal.installed_power.constants import (
    THERMAL_INSTALL_POWER_FOLDER,
    OutputThermalInstallPowerColumns,
)


class ThermalInstallerPowerParser:
    def __init__(self, output_folder: Path, main_params: MainParams, years: list[int]):
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

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

    def _get_start_and_end_timestamps_for_outputs(self) -> Iterator[pd.DatetimeIndex]:
        years = sorted(self.years)
        for year in years:
            start, end = get_starting_and_ending_timestamps_for_outputs(year)
            yield pd.date_range(start=start, end=end, freq="MS")  # MS = Month Start

    def _find_fuel(self, unit_name: str) -> str:
        for pattern, value in FUEL_MAPPING.items():
            if pattern in unit_name:
                return value
        return self.main_params.get_antares_cluster_technology_and_fuel(unit_name).fuel

    def _build_pegase_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        date_ranges = list(self._get_start_and_end_timestamps_for_outputs())

        start_dates = list(df[InputThermalColumns.COMMISSIONING_DATE])
        end_dates = list(df[InputThermalColumns.DECOMMISSIONING_DATE_EXPECTED])
        capacities = list(df[InputThermalColumns.NET_MAX_GEN_CAP])
        cluster_names = list(df[ANTARES_CLUSTER_NAME_COLUMN])
        node_names = list(df[ANTARES_NODE_NAME_COLUMN])

        cluster_groups: dict[str, dict[str, dict[pd.Timestamp, list[int | float]]]] = {}

        for k in range(len(df)):
            cluster_name = cluster_names[k]
            antares_node = node_names[k]

            cluster_groups.setdefault(antares_node, {}).setdefault(cluster_name, {})

            for date_range in date_ranges:
                for month in date_range:
                    if start_dates[k] <= month <= end_dates[k]:
                        cluster_groups[antares_node][cluster_name].setdefault(month, []).append(capacities[k])

        output_data: dict[str, list[Any]] = {
            OutputThermalInstallPowerColumns.AREA: [],
            OutputThermalInstallPowerColumns.FUEL: [],
            OutputThermalInstallPowerColumns.TECHNOLOGY: [],
            OutputThermalInstallPowerColumns.CLUSTER: [],
            OutputThermalInstallPowerColumns.CATEGORY: [],
        }
        for date_range in date_ranges:
            for month in date_range:
                month_as_string = month.strftime("%Y_%m")
                output_data[month_as_string] = []

        for area in sorted(cluster_groups):
            for cluster in sorted(cluster_groups[area]):
                # We have to handle `Bio` clusters as we don't have their mapping inside the `MainParams` class
                unit_name = cluster.removesuffix(f" {BIOMASS_CLUSTER_SUFFIX}")
                technology = self.main_params.get_antares_cluster_technology_and_fuel(unit_name).technology
                fuel = self._find_fuel(unit_name)
                output_data[OutputThermalInstallPowerColumns.AREA] += 2 * [area]
                output_data[OutputThermalInstallPowerColumns.FUEL] += 2 * [fuel]
                output_data[OutputThermalInstallPowerColumns.TECHNOLOGY] += 2 * [technology]
                output_data[OutputThermalInstallPowerColumns.CLUSTER] += 2 * [cluster]
                output_data[OutputThermalInstallPowerColumns.CATEGORY] += ["power", "number"]

                for date_range in date_ranges:
                    for month in date_range:
                        data = cluster_groups[area][cluster].get(month, [])
                        output_data[month.strftime("%Y_%m")] += [round(sum(data), MAX_DECIMAL_DIGITS), len(data)]

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

    def build_thermal_installed_power(self, df: pd.DataFrame) -> None:
        df = self._filter_columns_for_output(df)
        df = self._build_pegase_dataframe(df)
        self._export_dataframe(df)
