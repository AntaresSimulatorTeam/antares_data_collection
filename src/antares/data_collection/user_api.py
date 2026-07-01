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

from antares.data_collection.batteries.constants import (
    EFFICIENCY_INJECTION,
    OP_STAT_MARKET,
    OP_STAT_RESIDENTIAL,
    PEMMDB_PLANT_TYPE_MARKET,
    PEMMDB_PLANT_TYPE_RESIDENTIAL,
)
from antares.data_collection.batteries.parsing import BatteriesParser
from antares.data_collection.dsr.parsing import DsrParser
from antares.data_collection.links.constants import FILL_FOR_VALUES
from antares.data_collection.links.parsing import LinksParser
from antares.data_collection.misc.parsing import MiscParser
from antares.data_collection.referential_data.main_params import parse_main_params
from antares.data_collection.thermal.parsing import ThermalParser


class PEMMDBConverter:
    def __init__(self, input_folder: Path, output_folder: Path, main_params_path: Path, years: list[int]) -> None:
        self._input_folder = input_folder
        self._output_folder = output_folder
        self._main_params = parse_main_params(main_params_path)
        self._years = years

    def build_thermal_files(self, op_stat_values: list[str]) -> None:
        parser = ThermalParser(self._input_folder, self._output_folder, op_stat_values, self._main_params, self._years)
        parser.build_installed_power()
        parser.build_param_modulation()
        parser.build_specific_param()

    def build_dsr_files(self, op_stat_values: list[str], dsr_type_values: list[str], act_price_da: list[int]) -> None:
        parser = DsrParser(
            self._input_folder,
            self._output_folder,
            op_stat_values,
            dsr_type_values,
            act_price_da,
            self._main_params,
            self._years,
        )
        parser.build_dsr_cluster_part()
        parser.build_dsr_capacity_modulation_part()

    def build_misc_files(self, op_stat_values: list[str]) -> None:
        parser = MiscParser(self._input_folder, self._output_folder, op_stat_values, self._main_params, self._years)
        parser.build_misc_installed_power_part()
        parser.build_misc_load_factor_part()

    def build_link_files(self, for_limit_value: float = FILL_FOR_VALUES) -> None:
        parser = LinksParser(self._input_folder, self._output_folder, self._main_params, self._years, for_limit_value)
        parser.build_links()

    def build_batteries_files(
        self,
        pemmdb_plant_type_market: list[str] = PEMMDB_PLANT_TYPE_MARKET,
        op_stat_market: list[str] = OP_STAT_MARKET,
        pemmdb_plant_type_residential: list[str] = PEMMDB_PLANT_TYPE_RESIDENTIAL,
        op_stat_residential: list[str] = OP_STAT_RESIDENTIAL,
        efficiency_injection: float = EFFICIENCY_INJECTION,
    ) -> None:
        parser = BatteriesParser(
            self._input_folder,
            self._output_folder,
            self._main_params,
            self._years,
            pemmdb_plant_type_market,
            op_stat_market,
            pemmdb_plant_type_residential,
            op_stat_residential,
            efficiency_injection,
        )
        parser.build_batteries()
