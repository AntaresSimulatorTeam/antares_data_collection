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

from antares.data_collection.batteries.constants import BATTERIES_INPUT_FILE, InputBatteriesColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    parse_input_file,
)


class BatteriesParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        main_params: MainParams,
        years: list[int],
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years
        self.filtered_dataframe = self._build_filtered_batteries_dataframe()

    def _read_input_file_batteries(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(BATTERIES_INPUT_FILE), list(InputBatteriesColumns))

    def _build_filtered_batteries_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_batteries()
        df = filter_non_declared_areas(self.main_params, df, InputBatteriesColumns.MARKET_NODE)
        df = filter_based_on_study_scenarios(df, self.main_params, self.years, InputBatteriesColumns.STUDY_SCENARIO)
        df = filter_based_on_commission_date(
            df,
            self.years,
            InputBatteriesColumns.COMMISSIONING_DATE,
            InputBatteriesColumns.DECOMMISSIONING_DATE_EXPECTED,
        )
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.NET_MAX_CAP_GEN)
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.NET_MAX_CAP_DEM)
        df = filter_based_on_net_max_gen_cap(df, InputBatteriesColumns.STO_CAP)
        df = add_code_antares_colum(self.main_params, df, InputBatteriesColumns.MARKET_NODE)

        return df
