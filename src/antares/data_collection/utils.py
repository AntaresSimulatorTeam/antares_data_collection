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
import polars as pl

MAX_DECIMAL_DIGITS = 3


def write_csv_file(file_path: Path, df: pd.DataFrame) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    polars_df = pl.from_pandas(df)
    polars_df.write_csv(file_path, separator=",", float_precision=MAX_DECIMAL_DIGITS)
