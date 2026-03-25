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

from antares.data_collection.dsr.constants import DSR_INPUT_FILE, InputDsrColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.thermal.utils import parse_input_file


class DsrParser:
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
        self.filtered_dataframe = self._build_filtered_dsr_cluster_dataframe()

    def _read_input_file_dsr_cluster(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(DSR_INPUT_FILE), list(InputDsrColumns))

    def _build_filtered_dsr_cluster_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_dsr_cluster()
        # df = self._filter_values_based_on_op_stat(df)
        # df = self._filter_non_declared_areas(df)
        # df = filter_input_based_on_study_scenarios(df, self.main_params, self.years)
        # df = filter_thermal_input_file_based_on_commission_date(df, self.years)
        # df = self._add_antares_cluster_name_colum(df)
        # df = self._split_clusters_with_biomass_rule(df)
        # df = self._filter_values_based_on_net_max_gen_cap(df)
        # df =  self._add_code_antares_colum(df)
        return df

    def build_dsr_cluster(self) -> None:
        pass
