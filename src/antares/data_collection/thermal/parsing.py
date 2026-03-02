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

from antares.data_collection.referential_data.main_params import MainParams, parse_main_params
from antares.data_collection.thermal.constants import (
    BIOMASS_SNCD_FUEL,
    DEFAULT_DECOMMISSIONING_DATE,
    THERMAL_INPUT_FILE,
    InputThermalColumns,
)

CLUSTER_NAME_INTERMEDIATE_COLUMN = "cluster_name"


class ThermalParser:
    def __init__(self, folder_path: Path, op_stat_values: list[str], main_params: MainParams, years: list[int]):
        self.folder_path = folder_path
        self.op_stat_values = op_stat_values
        self.main_params = main_params
        self.years = years

    def _read_input_file(self) -> pd.DataFrame:
        input_file_path = self.folder_path.joinpath(THERMAL_INPUT_FILE)
        if not input_file_path.exists():
            raise ValueError(f"Thermal input file {input_file_path} not found")

        # Checks that all expected columns exist
        df = pd.read_csv(input_file_path)
        existing_cols = set(df.columns)
        expected_cols = [col for col in InputThermalColumns]
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
        scenario_types = self.main_params.get_scenario_types(years=self.years)

        if not scenario_types:
            return df

        if len(scenario_types) == 2:
            # The input writing is `X&Y` so we have to consider that
            scenario_types.append(f"{scenario_types[0]}&{scenario_types[1]}")

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
        df[CLUSTER_NAME_INTERMEDIATE_COLUMN] = self.main_params.get_clusters_bp(cluster_list)
        return df

    def _split_clusters_with_biomass_rule(self, df: pd.DataFrame) -> pd.DataFrame:
        fuels = df[InputThermalColumns.SCND_FUEL]
        cluster_names = list(df[CLUSTER_NAME_INTERMEDIATE_COLUMN])
        for k, fuel in enumerate(fuels):
            if fuel == BIOMASS_SNCD_FUEL:
                print(cluster_names[k])
        return df

    def build_thermal_installed_power(self) -> pd.DataFrame:
        input_df = self._read_input_file()
        df = self._filter_values_based_on_op_stat(input_df)
        df = self._filter_values_based_on_study_scenarios(df)
        df = self._filter_values_based_on_net_max_gen_cap(df)
        df = self._filter_values_based_on_commission_date(df)
        df = self._add_antares_cluster_name_colum(df)
        df = self._split_clusters_with_biomass_rule(df)
        """
        TODO:
        - Then Bio / Fuel units as we have to add suffix to the name
        - Convert areas to their Antares names
        - Write the ouput file
        """
        return df


def test_truc():
    resource_path = Path("/home/belthlemar/Projects/Antares/antares_data_collection/tests/antares/resources")
    main_params = parse_main_params(resource_path / "MAIN_PARAMS_2025.xlsx")
    parser = ThermalParser(resource_path, ["Available on market"], main_params, [2030])
    df = parser.build_thermal_installed_power()
    print(df)
    """final_df = pd.read_csv(resource_path / "expected_output_files" / "thermal_installed_power.csv")
    print(final_df)
    """
