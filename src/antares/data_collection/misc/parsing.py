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

from antares.data_collection.misc.constants import MISC_INPUT_FILE, InputMiscColumns
from antares.data_collection.misc.installed_power.parsing import MiscInstalledPowerParser
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_antares_cluster_name_colum,
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    filter_non_declared_clusters,
    parse_input_file,
)


class MiscParser:
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
        return parse_input_file(self.input_folder.joinpath(MISC_INPUT_FILE), list(InputMiscColumns))

    def _build_filtered_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file()
        df = filter_based_on_op_stat(self.op_stat_values, df, InputMiscColumns.OP_STAT)
        df = filter_non_declared_areas(self.main_params, df, InputMiscColumns.MARKET_NODE)
        df = filter_non_declared_clusters(self.main_params, df, InputMiscColumns.PEMMDB_PLANT_TYPE)
        df = filter_based_on_study_scenarios(df, self.main_params, self.years, InputMiscColumns.STUDY_SCENARIO)
        df = filter_based_on_commission_date(
            df,
            self.years,
            InputMiscColumns.COMMISSIONING_DATE,
            InputMiscColumns.DECOMMISSIONING_DATE_EXPECTED,
        )
        df = add_antares_cluster_name_colum(self.main_params, df, InputMiscColumns.PEMMDB_PLANT_TYPE)
        df = filter_based_on_net_max_gen_cap(df, InputMiscColumns.NET_MAX_GEN_CAP.value)
        return add_code_antares_colum(self.main_params, df, InputMiscColumns.MARKET_NODE.value)

    def build_misc_installed_power_part(self) -> None:
        parser = MiscInstalledPowerParser(self.output_folder, self.main_params, self.years)
        parser.build_misc_installed_power(self._build_filtered_dataframe())
