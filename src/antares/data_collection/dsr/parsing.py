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

from antares.data_collection.dsr.capacity_modulation.parsing import DsrCapacityModulationParser
from antares.data_collection.dsr.cluster.parsing import DsrClusterParser
from antares.data_collection.dsr.constants import (
    DSR_INPUT_FILE,
    InputDsrColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_commission_date,
    filter_based_on_net_max_gen_cap,
    filter_based_on_op_stat,
    filter_based_on_study_scenarios,
    filter_non_declared_areas,
    parse_input_file,
)


class DsrParser:
    def __init__(
        self,
        input_folder: Path,
        output_folder: Path,
        op_stat_values: list[str],
        dsr_type_values: list[str],
        act_price_da: list[int],
        main_params: MainParams,
        years: list[int],
    ):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.op_stat_values = op_stat_values
        self.dsr_type_values = dsr_type_values
        self.act_price_da = act_price_da
        self.main_params = main_params
        self.years = years
        self.filtered_dataframe = self._build_filtered_dsr_cluster_dataframe()

    def _read_input_file_dsr_cluster(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(DSR_INPUT_FILE), list(InputDsrColumns))

    def _filter_based_on_dsr_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """We want to keep only the lines where the DSR_TYPE value matches the user given ones"""
        dsr_type_values = self.dsr_type_values
        if not dsr_type_values:
            return df
        df = df[df[InputDsrColumns.DSR_TYPE].isin(dsr_type_values)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"The given dsr_type values {dsr_type_values} are not present in the dataframe")
        return df

    def _filter_out_based_on_act_price_da(self, df: pd.DataFrame) -> pd.DataFrame:
        """We want to exclude only the lines where the ACT_PRICE_DA value matches the user given ones"""
        act_price_da = self.act_price_da
        if not act_price_da:
            return df
        df = df[~df[InputDsrColumns.ACT_PRICE_DA].isin(act_price_da)]
        if df.empty:
            # We want to raise as soon as possible to have a clear error msg
            raise ValueError(f"The given act_price_da values {act_price_da} exclude all row in the dataframe")
        return df

    def _build_filtered_dsr_cluster_dataframe(self) -> pd.DataFrame:
        df = self._read_input_file_dsr_cluster()
        df = filter_based_on_op_stat(self.op_stat_values, df, InputDsrColumns.OP_STAT.value)
        df = self._filter_based_on_dsr_type(df)
        df = self._filter_out_based_on_act_price_da(df)
        df = filter_non_declared_areas(self.main_params, df, InputDsrColumns.MARKET_NODE.value)
        df = filter_based_on_study_scenarios(df, self.main_params, self.years, InputDsrColumns.STUDY_SCENARIO.value)
        df = filter_based_on_commission_date(
            df,
            self.years,
            InputDsrColumns.COMMISSIONING_DATE.value,
            InputDsrColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        )
        df = filter_based_on_net_max_gen_cap(df, InputDsrColumns.NET_MAX_GEN_CAP.value)
        df = add_code_antares_colum(self.main_params, df, InputDsrColumns.MARKET_NODE.value)

        return df

    def build_dsr_cluster_part(self) -> None:
        parser = DsrClusterParser(self.output_folder, self.main_params, self.years)
        parser.build_dsr_cluster(self.filtered_dataframe)

    def build_dsr_capacity_modulation_part(self) -> None:
        parser = DsrCapacityModulationParser(self.input_folder, self.output_folder, self.main_params, self.years)
        parser.build_dsr_capacity_modulation(self.filtered_dataframe)
