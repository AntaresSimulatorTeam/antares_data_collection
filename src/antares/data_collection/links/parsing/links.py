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

from antares.data_collection.links.constants import NTC_FILE_NAME, NTC_INDEX_FILE_NAME, NtcIndexColumns
from antares.data_collection.referential_data.main_params import MainParams


class LinksParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years


    def _read_ntc_file(self) -> pd.DataFrame:
        input_file_path = self.input_folder.joinpath(NTC_FILE_NAME)
        if not input_file_path.exists():
            raise ValueError(f"Links NTC input file {input_file_path} not found")

        # Checks that all expected columns exist
        df = pd.read_csv(input_file_path)

        # We need all columns inside this file
        return df

    def _fill_ntc_index_map(self) -> None:
        # First read the file
        input_file_path = self.input_folder.joinpath(NTC_INDEX_FILE_NAME)
        if not input_file_path.exists():
            raise ValueError(f"Thermal input file {input_file_path} not found")

        # Checks that all expected columns exist
        df = pd.read_csv(input_file_path)
        existing_cols = set(df.columns)
        expected_cols = list(NtcIndexColumns)
        for expected_column in expected_cols:
            if expected_column not in existing_cols:
                raise ValueError(f"Column {expected_column} not found in {input_file_path}")

        # Keep the useful dataframe columns only
        df = df[expected_cols]
