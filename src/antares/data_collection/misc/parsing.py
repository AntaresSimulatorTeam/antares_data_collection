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
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import parse_input_file

# TODO
# context
# one part to compute capacity of clusters with an excel workbook exported (installed power)
# one part to compute weight time series (load factor)

# the internal folder structure will be similar like thermal/dsr with the main class and one method to build specific export


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

    def _read_input_file(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder.joinpath(MISC_INPUT_FILE), list(InputMiscColumns))
