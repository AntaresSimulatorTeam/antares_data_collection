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
    DEFAULT_DECOMMISSIONING_DATE,
    InputThermalColumns,
)
from antares.data_collection.utils import ANTARES_NODE_NAME_COLUMN
from antares.data_collection.thermal.constants import DEFAULT_DECOMMISSIONING_DATE, InputThermalColumns
from antares.data_collection.thermal.param_modulation.constants import CAPACITY_MODULATION_NAME, TECHNICAL_PARAMS_FOLDER


def get_path_capacity_modulation_file(year: int, root_export_folder: Path) -> Path:
    name_file = f"{CAPACITY_MODULATION_NAME}_{year - 1}-{year}.csv"
    full_path_file = root_export_folder / TECHNICAL_PARAMS_FOLDER / name_file
    return full_path_file


def parse_input_file(input_file_path: Path, expected_columns: list[str]) -> pd.DataFrame:
    if not input_file_path.exists():
        raise ValueError(f"File {input_file_path} not found")

    # Checks that all expected columns exist
    df = pd.read_csv(input_file_path)
    existing_cols = set(df.columns)
    for expected_column in expected_columns:
        if expected_column not in existing_cols:
            raise ValueError(f"Column {expected_column} not found in {input_file_path}")

    # Keep useful columns only
    return df[expected_columns]


def get_starting_and_ending_timestamps_for_outputs(year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Implicit rule: For a given year, we have to consider the year starts in July of the previous year
    and ends in June of the current year.
    Example: 2030 : 1st July 2029 -> 30 June 2030
    """
    return pd.Timestamp(year - 1, 7, 1), pd.Timestamp(year, 6, 30)


@dataclass
class CommissioningDateLimits:
    last_possible_commissioning_date: pd.Timestamp
    earliest_possible_decommissioning_date: pd.Timestamp


def get_starting_and_ending_timestamps(years: list[int]) -> Iterator[CommissioningDateLimits]:
    """
    For each year in `years`, we should consider:
    - 31st December of the year -> Each cluster with a commissioning date after this will not be considered.
    - 1st January of previous year -> Each cluster with a decommissioning date before this will not be considered.
    """
    for year in years:
        yield CommissioningDateLimits(
            last_possible_commissioning_date=pd.Timestamp(year=year, month=12, day=31),
            earliest_possible_decommissioning_date=pd.Timestamp(year=year - 1, month=1, day=1),
        )


def filter_thermal_input_file_based_on_commission_date(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    if not years:
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

    commissioning_limits = list(get_starting_and_ending_timestamps(years))
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
        msg = f"No input data matched the given (de)commissioning dates for the given years {years}"
        raise ValueError(msg)
    return df


def filter_input_based_on_study_scenarios(df: pd.DataFrame, main_params: MainParams, years: list[int]) -> pd.DataFrame:
    """
    Using MainParams and the user given years, we retrieve the study scenarios we have to consider.
    Other scenarios present in the input file will be ignored.
    """
    scenario_types = list(main_params.get_scenario_types(years=years))

    if not scenario_types:
        return df

    df = df[df[InputThermalColumns.STUDY_SCENARIO].str.contains("|".join(scenario_types), case=False, na=False)]
    if df.empty:
        # We want to raise as soon as possible to have a clear error msg
        raise ValueError(f"No input data matched the given study scenario for the given years {years}")
    return df


def filter_non_declared_areas(main_params: MainParams, df: pd.DataFrame) -> pd.DataFrame:
    """
    Some nodes are not inside RTE study perimeter and therefore not registered inside the main parameters file.
    We don't want to consider them.
    We simply log a message for each area we find in this case
    """
    all_market_nodes = set(df[InputThermalColumns.MARKET_NODE])
    missing_nodes = []
    for node in all_market_nodes:
        antares_code = main_params.get_antares_code(node)
        if not antares_code:
            missing_nodes.append(node)

    if missing_nodes:
        return df[~df[InputThermalColumns.MARKET_NODE].isin(missing_nodes)]
    return df


def add_antares_cluster_name_colum(main_params: MainParams, df: pd.DataFrame) -> pd.DataFrame:
    cluster_list = df[InputThermalColumns.PEMMDB_TECHNOLOGY].tolist()
    df[ANTARES_CLUSTER_NAME_COLUMN] = main_params.get_clusters_bp(cluster_list)
    return df


def filter_values_based_on_net_max_gen_cap(df: pd.DataFrame) -> pd.DataFrame:
    """We do not consider clusters with a `NET_MAX_GEN_CAP` of 0."""
    return df.loc[df[InputThermalColumns.NET_MAX_GEN_CAP] > 0]


def add_code_antares_colum(main_params: MainParams, df: pd.DataFrame) -> pd.DataFrame:
    node_list = df[InputThermalColumns.MARKET_NODE].tolist()
    df[ANTARES_NODE_NAME_COLUMN] = main_params.get_antares_codes(node_list)
    return df


def apply_round_to_numeric_columns(
    df: pd.DataFrame, numeric_columns: list[str], decimals_precision: int = 0
) -> pd.DataFrame:
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Column {col} must be numeric")
    df[numeric_columns] = df[numeric_columns].round(decimals_precision)
    return df
