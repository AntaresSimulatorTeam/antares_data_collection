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
from typing import Any

from antares.data_collection.links import conf_links

import pandas as pd


def create_links_part(dir_input: Path, dir_output: Path, **kwargs: Any) -> None:
    # check input/output directory
    if not dir_input.is_dir():
        raise ValueError(f"Input directory {dir_input} does not exist.")

    if not dir_output.is_dir():
        raise ValueError(f"Output directory {dir_output} does not exist.")

    # check is files needed are present
    for file_name in conf_links.LinksFileNames().files:
        path_file = dir_input / file_name
        if not path_file.exists():
            raise ValueError(f"Input file does not exist: {path_file}")

    # read files
    results = {}
    for file_name in conf_links.LinksFileNames().files:
        full_path = dir_input / file_name
        df = pd.read_csv(full_path)
        results[file_name] = df

    # compute median group by HP/HC & Winter/Summer
        # use ref "peak" to tag and grouping then
    df_ts_ntc = results['NTCs.csv'].copy()
    ref_hours = kwargs['ref_params']['ref_peak_hours']
    ref_months = kwargs['ref_params']['ref_peak_months']

    # merge hours/saison
        # NO DOC/NO Autocompletion df_ts_ntc.merge(ref_hours, left_on="HOUR", right_on="hour", how="left")
    df_ts_ntc = pd.merge(df_ts_ntc, ref_hours, left_on="HOUR", right_on="hour", how="left")
    df_ts_ntc = pd.merge(df_ts_ntc, ref_months, left_on="MONTH", right_on="month", how="left")
    df_ts_ntc = df_ts_ntc.drop(columns=["hour", "month", "MONTH", "DAY", "HOUR"])

    # compute median hours/saison
    df_median_grouped = df_ts_ntc.groupby(by=['period_hour', 'period_month'], as_index=False).median()
    df_median_tot = df_ts_ntc.median(numeric_only=True)

    # TODO add col name MEDIAN (df_median_tot) and put long format and rename columns (df_median_grouped)


    # merge those median with ntc index
    df_ts_ntc_index = results['NTCs Index.csv'].copy()
        # TODO df_ts_ntc_index.merge(df_median_grouped, left_on="CURVE_UID", right_on="NTC", how="left")


    # export part
