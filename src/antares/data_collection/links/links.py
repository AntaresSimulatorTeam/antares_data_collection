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


from antares.data_collection.links import conf_links

import pandas as pd
from typing import List, Optional

from antares.data_collection.tools.conf import LocalConfiguration


def create_links_part(conf_input: LocalConfiguration) -> None:
    # check files required
    conf_links_files = conf_links.LinksFileConfig()
    for file_name in conf_links_files.all_names():
        path_file = conf_input.input_path / file_name
        if not path_file.exists():
            raise ValueError(f"Input file does not exist: {path_file}")

    # read files
    results = {}
    for file_name in conf_links_files.all_names():
        full_path = conf_input.input_path / file_name
        df = pd.read_csv(full_path)
        results[file_name] = df

    # region
    # NTC TS + INDEX
    # computes a median group by HP/HC & Winter/Summer
    # use ref "peak" to tag and grouping then
    df_ts_ntc = results[conf_links_files.NTC_TS].copy()

    # read references .xlsx files
    ref_peak = pd.read_excel(conf_input.data_references_path, sheet_name="PEAK_PARAMS")
    ref_hours = ref_peak[["hour", "period_hour"]]
    ref_months = ref_peak[["month", "period_month"]]

    # merge hours/saison
    # NO DOC/NO Autocompletion df_ts_ntc.merge(ref_hours, left_on="HOUR", right_on="hour", how="left")
    df_ts_ntc = pd.merge(
        df_ts_ntc, ref_hours, left_on="HOUR", right_on="hour", how="left"
    )

    df_ts_ntc = pd.merge(
        df_ts_ntc, ref_months, left_on="MONTH", right_on="month", how="left"
    )
    df_ts_ntc = df_ts_ntc.drop(columns=["hour", "month", "MONTH", "DAY", "HOUR"])

    # compute median hours/saison
    df_median_grouped = df_ts_ntc.groupby(
        by=["period_hour", "period_month"], as_index=False
    ).median()
    series_median = df_ts_ntc.median(numeric_only=True)

    # retreatment + pivot to merge
    df_median_tot = pd.DataFrame(
        {"CURVE_UID": series_median.index, "MEDIAN": series_median.values}
    )
    df_median_grouped["colname"] = (
        df_median_grouped["period_month"]
        .astype(str)
        .str.cat(df_median_grouped["period_hour"].astype(str), sep="_")
    )
    df_median_grouped.colname = df_median_grouped.colname.str.upper()
    df_median_grouped = df_median_grouped.drop(columns=["period_month", "period_hour"])

    df_pivot = (
        df_median_grouped.set_index("colname")
        .T.reset_index()
        .rename(columns={"index": "CURVE_UID"})
    )

    # df with all computed medians by curve_id
    df_ts_median = pd.merge(df_pivot, df_median_tot, how="left")

    # merge median with ntc index
    df_ts_ntc_index = (
        results[conf_links_files.NTC_INDEX].copy().drop(columns=["LABEL", "COUNT"])
    )
    df_ts_ntc_index = pd.merge(
        df_ts_ntc_index, df_ts_median, on="CURVE_UID", how="left"
    )
    # endregion

    # region
    # Transfer capacity
    # global filter `TRANSFER_TYPE` = NTC + `TRANSFER_TECHNOLOGY` = HVAC
    df_transfer = results[conf_links_files.TRANSFER_LINKS].copy()
    df_transfer = df_transfer.loc[
        (df_transfer["TRANSFER_TYPE"] == "NTC")
        & (df_transfer["TRANSFER_TECHNOLOGY"] == "HVAC")
    ]

    # merge data with computed median
    df_transfer = pd.merge(
        df_transfer,
        df_ts_ntc_index,
        left_on=["ZONE", "NTC_CURVE_ID"],
        right_on=["ZONE", "ID"],
        how="left",
    ).drop(columns=["ID", "CURVE_UID"])

    # merge column 'code_antares' :
    # for market zone source and market zone destination
    ref_country_links = pd.read_excel(
        conf_input.data_references_path, sheet_name="LINKS"
    )

    # source
    df_transfer = pd.merge(
        df_transfer,
        ref_country_links,
        left_on="MARKET_ZONE_SOURCE",
        right_on="market_node",
        how="left",
    )
    df_transfer.drop(columns=["market_node"], inplace=True)
    df_transfer.rename(columns={"code_antares": "code_source"}, inplace=True)

    # destination
    df_transfer = pd.merge(
        df_transfer,
        ref_country_links,
        left_on="MARKET_ZONE_DESTINATION",
        right_on="market_node",
        how="left",
    )
    df_transfer.drop(columns=["market_node"], inplace=True)
    df_transfer.rename(columns={"code_antares": "code_destination"}, inplace=True)

    # treatment for calendar year
    # filter with scenario and calendar year
    year_param = conf_input.calendar_year
    ref_scenario = pd.read_excel(
        conf_input.data_references_path, sheet_name="STUDY_SCENARIO"
    )

    d_df_year = {}
    for iyear in year_param:
        # filter scenario
        scenario_values = ref_scenario.loc[
            ref_scenario["YEAR"].isin([iyear])
        ].STUDY_SCENARIO.item()
        df_transfer_year = scenario_filter(
            df_input=df_transfer, filter_params=scenario_values
        )
        # filter by year
        df_transfer_year = df_transfer.loc[
            (df_transfer["YEAR_VALID_START"] <= iyear)
            & (df_transfer["YEAR_VALID_END"] >= iyear)
        ]

        d_df_year[str(iyear)] = df_transfer_year

    d_df_year.keys()
    # endregion

    # export part


# TODO add in folder "tools" then + add tests
def scenario_filter(
    df_input: pd.DataFrame, filter_params: Optional[List[str]] = None
) -> pd.DataFrame:
    valid_choices: List[str] = ["ERAA", "TYNDP"]

    # default: "ERAA"
    if filter_params is None or len(filter_params) != 1:
        filter_params = ["ERAA"]

    fp: str = filter_params[0]

    if fp not in valid_choices:
        raise ValueError(f"filter_params must be in {valid_choices}")

    filter_map: dict[str, str] = {
        "ERAA": r"ERAA&TYNDP|ERAA",
        "TYNDP": r"ERAA&TYNDP|TYNDP",
    }

    pattern: str = filter_map[fp]

    return df_input[df_input["STUDY_SCENARIO"].str.contains(pattern, regex=True)]
