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
import polars as pl
import xlsxwriter  # type: ignore[import-untyped]

from antares.data_collection.constants import (
    ANTARES_NODE_NAME_COLUMN,
    DEFAULT_DECOMMISSIONING_DATE,
    MAX_DECIMAL_DIGITS,
)
from antares.data_collection.referential_data.main_params import MainParams


def write_csv_file(file_path: Path, df: pd.DataFrame) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    polars_df = pl.from_pandas(df)
    polars_df.write_csv(file_path, separator=",", float_precision=MAX_DECIMAL_DIGITS)


def filter_based_on_op_stat(filter_op_stat_values: list[str], df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """We want to keep only the lines were the OP_STAT value matches the user given ones"""
    if not filter_op_stat_values:
        return df
    if column_name not in df.columns:
        raise ValueError(f"Column {column_name} not found in the dataframe")
    df = df[df[column_name].isin(filter_op_stat_values)]
    if df.empty:
        # We want to raise as soon as possible to have a clear error msg
        raise ValueError(f"The given op_stat values {filter_op_stat_values} are not present in the dataframe")
    return df


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


def filter_based_on_commission_date(
    df: pd.DataFrame,
    years: list[int],
    commissioning_name_column: str,
    decommissioning_name_column: str,
    default_decommissioning_date: pd.Timestamp = DEFAULT_DECOMMISSIONING_DATE,
) -> pd.DataFrame:
    if not years:
        return df

    for column in [commissioning_name_column, decommissioning_name_column]:
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in the dataframe")

    # Dates objects are stored as Strings for the moment, we have to change this to perform checks.
    df[commissioning_name_column] = pd.to_datetime(df[commissioning_name_column])

    # Some values might be missing inside `decommissioning_name_column`.
    # If so, we should consider the decommissioning year to be 2100.
    df[decommissioning_name_column] = pd.to_datetime(df[decommissioning_name_column]).fillna(
        value=default_decommissioning_date
    )

    # Reindex the dataframe to use Series freely
    df.index = pd.RangeIndex(len(df))

    commissioning_limits = list(get_starting_and_ending_timestamps(years))
    start_dates = df[commissioning_name_column]
    end_dates = df[decommissioning_name_column]
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


def filter_based_on_study_scenarios(
    df: pd.DataFrame, main_params: MainParams, years: list[int], study_scenario_name_column: str
) -> pd.DataFrame:
    """
    Using MainParams and the user given years, we retrieve the study scenarios we have to consider.
    Other scenarios present in the input file will be ignored.
    """
    scenario_types = list(main_params.get_scenario_types(years=years))

    if not scenario_types:
        return df

    if study_scenario_name_column not in df.columns:
        raise ValueError(f"Column {study_scenario_name_column} not found in the dataframe")

    df = df[df[study_scenario_name_column].str.contains("|".join(scenario_types), case=False, na=False)]
    if df.empty:
        # We want to raise as soon as possible to have a clear error msg
        raise ValueError(f"No input data matched the given study scenario for the given years {years}")
    return df


def filter_non_declared_areas(main_params: MainParams, df: pd.DataFrame, market_node_name_column: str) -> pd.DataFrame:
    """
    Some nodes are not inside RTE study perimeter and therefore not registered inside the main parameters file.
    We don't want to consider them.
    We simply log a message for each area we find in this case
    """
    if market_node_name_column not in df.columns:
        raise ValueError(f"Column {market_node_name_column} not found in the dataframe")

    all_market_nodes = set(df[market_node_name_column])
    missing_nodes = []
    for node in all_market_nodes:
        antares_code = main_params.get_antares_code(node)
        if not antares_code:
            missing_nodes.append(node)

    if missing_nodes:
        return df[~df[market_node_name_column].isin(missing_nodes)]
    return df


def filter_based_on_net_max_gen_cap(df: pd.DataFrame, net_max_gen_cap_name_column: str) -> pd.DataFrame:
    """We do not consider clusters with a `NET_MAX_GEN_CAP` of 0."""
    if net_max_gen_cap_name_column not in df.columns:
        raise ValueError(f"Column {net_max_gen_cap_name_column} not found in the dataframe")
    return df.loc[df[net_max_gen_cap_name_column] > 0]


def filter_out_based_on_year(
    df: pd.DataFrame, year: int, commissioning_name_column: str, decommissioning_name_column: str
) -> pd.DataFrame:
    """This function only keeps rows where the unit is commissioned on the 1st January of the given year."""
    date = pd.Timestamp(year=year, month=1, day=1)
    mask = (df[commissioning_name_column] <= date) & (df[decommissioning_name_column] >= date)

    return df.loc[mask]


def add_code_antares_colum(main_params: MainParams, df: pd.DataFrame, market_node_name_column: str) -> pd.DataFrame:
    node_list = df[market_node_name_column].tolist()
    df[ANTARES_NODE_NAME_COLUMN] = main_params.get_antares_codes(node_list)
    return df


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


def write_excel_workbook(
    file_path: Path,
    dataframes_by_sheet: dict[str, pd.DataFrame],
) -> None:
    """
    Write multiple pandas DataFrames to an Excel file using xlsxwriter.

    Each key in the dictionary corresponds to a sheet name.

    Args:
        file_path (str):
            Path to the output Excel file (will be overwritten if exists).

        dataframes_by_sheet (Dict[str, pd.DataFrame]):
            Dictionary where:
                - key = sheet name (str)
                - value = pandas DataFrame to write in the sheet

    Returns:
        None
    """
    workbook = xlsxwriter.Workbook(file_path)

    try:
        for sheet_name, df in dataframes_by_sheet.items():
            worksheet = workbook.add_worksheet(sheet_name)

            # Write headers
            worksheet.write_row(0, 0, df.columns.tolist())

            # Write data
            for row_num, row in enumerate(df.values, start=1):
                worksheet.write_row(row_num, 0, row)

    finally:
        workbook.close()


def filter_index_files_with_scenario_year(
    main_params: MainParams, df: pd.DataFrame, year: int, filter_scenario_value: str, target_year_col: str
) -> pd.DataFrame:
    scenario = main_params.get_scenario_type(year=year)
    acceptable_scenario_types = [filter_scenario_value, f"{scenario}_{year}", f"All_years_{scenario}"]
    return df[df[target_year_col].isin(acceptable_scenario_types)]


def insert_str_date_time_reindex(df: pd.DataFrame, year: int, datetime_column_name: str) -> pd.DataFrame:
    # We want our dataframe to start on the 1st of July at midnight for PEGASE.
    # So we have to reindex it at the right index
    new_df = df.copy()
    starting_time = pd.Timestamp(year=year - 1, month=7, day=1, hour=0)
    time_delta = starting_time - pd.Timestamp(year=year - 1, month=1, day=1, hour=0)
    first_index = time_delta.days * 24
    new_index = list(range(first_index, len(new_df))) + list(range(0, first_index))
    reindex_df = new_df.reindex(new_index)

    # Add the `Date` column
    date_values = [str(starting_time + pd.Timedelta(hours=i)) for i in range(len(reindex_df))]
    reindex_df.insert(0, datetime_column_name, date_values)

    return reindex_df


def filter_based_on_year_range(
    df: pd.DataFrame,
    years: list[int],
    start_column: str,
    end_column: str,
) -> pd.DataFrame:
    """
    Keep rows where at least one year in `years` satisfies:
    start_column <= year <= end_column
    """
    if not years:
        return df

    for column in [start_column, end_column]:
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in the dataframe")

    # Create a mask that is True if any of the requested years are within the range
    mask = pd.Series(False, index=df.index)
    for year in years:
        mask |= (df[start_column] <= year) & (df[end_column] >= year)

    df = df[mask]

    if df.empty:
        raise ValueError(f"No input data matched the given years {years} in range {start_column} - {end_column}")

    return df
