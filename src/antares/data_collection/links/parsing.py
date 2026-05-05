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

from antares.data_collection.links.constants import (
    LINKS_TRANSFER_LINKS_NAME,
    NTC_FILTER_STR_VALUE,
    InputTransferLinksColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_based_on_study_scenarios,
    filter_based_on_year_range,
    filter_non_declared_areas,
    parse_input_file,
)


class LinksParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _parse_transfer_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_TRANSFER_LINKS_NAME, list(InputTransferLinksColumns))

    def _filter_based_on_ntc(self, ntc_name_column: str, df: pd.DataFrame) -> pd.DataFrame:
        if ntc_name_column not in df.columns:
            raise ValueError(f"Column {ntc_name_column} not found in the dataframe 'Transfer Links'")
        return df[df[ntc_name_column] == NTC_FILTER_STR_VALUE]

    def build_links(self) -> None:
        df = self._parse_transfer_links()

        # transfer links file pre filter
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        df = filter_based_on_study_scenarios(
            df, self.main_params, self.years, InputTransferLinksColumns.STUDY_SCENARIO.value
        )
        df = filter_based_on_year_range(
            df,
            self.years,
            InputTransferLinksColumns.YEAR_VALID_START.value,
            InputTransferLinksColumns.YEAR_VALID_END.value,
        )
        df = self._filter_based_on_ntc(InputTransferLinksColumns.TRANSFER_TYPE, df)

        # mapping with index/time series
