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

import pandas as pd

from antares.data_collection import LocalConfiguration
from antares.data_collection.thermal.conf_thermal import (
    ThermalLayout,
    ThermalDataColumns,
)


# TODO steps of thermal process


# manage only one file "Thermal.csv"
def thermal_import(conf_input: LocalConfiguration) -> pd.DataFrame:
    # check files required
    conf_thermal_file = ThermalLayout()
    file_name = conf_thermal_file.input_data_name
    path_file = conf_input.input_path / file_name

    if not path_file.exists():
        raise ValueError(f"Input file does not exist: {path_file}")

    # read a file with only columns used
    list_col_to_use = [col.value for col in ThermalDataColumns]
    df = pd.read_csv(filepath_or_buffer=path_file, usecols=list_col_to_use)

    if df.empty:
        raise ValueError(f"Input file is empty: {path_file}")

    return df


# TODO next steps
def thermal_pre_treatments() -> None:
    raise NotImplementedError("Not implemented yet")


def thermal_treatments_year() -> None:
    raise NotImplementedError("Not implemented yet")


def thermal_export() -> None:
    raise NotImplementedError("Not implemented yet")
