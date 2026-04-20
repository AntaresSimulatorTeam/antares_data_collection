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
from antares.data_collection.thermal.constants import ANTARES_CLUSTER_NAME_COLUMN, InputThermalColumns
from antares.data_collection.thermal.param_modulation.constants import CAPACITY_MODULATION_NAME, TECHNICAL_PARAMS_FOLDER


def get_path_capacity_modulation_file(year: int, root_export_folder: Path) -> Path:
    name_file = f"{CAPACITY_MODULATION_NAME}_{year - 1}-{year}.csv"
    full_path_file = root_export_folder / TECHNICAL_PARAMS_FOLDER / name_file
    return full_path_file


def get_starting_and_ending_timestamps_for_outputs(year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Implicit rule: For a given year, we have to consider the year starts in July of the previous year
    and ends in June of the current year.
    Example: 2030 : 1st July 2029 -> 30 June 2030
    """
    return pd.Timestamp(year - 1, 7, 1), pd.Timestamp(year, 6, 30)


def apply_round_to_numeric_columns(
    df: pd.DataFrame, numeric_columns: list[str], decimals_precision: int = 0
) -> pd.DataFrame:
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Column {col} must be numeric")
    df[numeric_columns] = df[numeric_columns].round(decimals_precision)
    return df


def add_antares_cluster_name_colum(main_params: MainParams, df: pd.DataFrame) -> pd.DataFrame:
    cluster_list = df[InputThermalColumns.PEMMDB_TECHNOLOGY].tolist()
    df[ANTARES_CLUSTER_NAME_COLUMN] = main_params.get_clusters_bp(cluster_list)
    return df
