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
import numpy as np

from antares.data_collection.links import conf_links

import pandas as pd

from antares.data_collection.tools.conf import LocalConfiguration

# Data referential
from antares.data_collection.links.conf_links import (
    ReferentialSheetNames as RefSheetNames,
)
from antares.data_collection.links.conf_links import PeakParamsColumnsNames as RefPeak

# Data Links
from antares.data_collection.links.conf_links import NTCS

# internal function(s)
from antares.data_collection.tools import tools


def links_data_management(conf_input: LocalConfiguration) -> dict[str, pd.DataFrame]:
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
    ref_peak = pd.read_excel(
        conf_input.data_references_path, sheet_name=RefSheetNames.PEAK_PARAMS.value
    )
    ref_hours = ref_peak[[RefPeak.HOUR.value, RefPeak.PERIOD_HOUR.value]]
    ref_months = ref_peak[[RefPeak.MONTH.value, RefPeak.PERIOD_MONTH.value]]

    # merge hours/saison
    df_ts_ntc = pd.merge(
        df_ts_ntc, ref_hours, left_on=NTCS.HOUR, right_on=RefPeak.HOUR.value, how="left"
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
    df_median_grouped["colname"] = df_median_grouped["colname"].str.upper()
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

    # delete row with NAN
    df_transfer.dropna(subset=["code_source", "code_destination"], inplace=True)

    # ADD new column "border" to combine code source + destination
    df_transfer["border"] = (
        df_transfer["code_source"] + "-" + df_transfer["code_destination"]
    )

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
        df_transfer_year = tools.scenario_filter(
            df_input=df_transfer, filter_params=scenario_values
        )
        # filter by year
        df_transfer_year = df_transfer_year.loc[
            (df_transfer_year["YEAR_VALID_START"] <= iyear)
            & (df_transfer_year["YEAR_VALID_END"] >= iyear)
        ]

        # TODO if data frame is empty skip/pass the year of treatment

        ## multi GRT treatment

        # identify
        borders = (
            df_transfer_year.groupby(["ZONE", "border"])
            .size()
            .reset_index(name="N")
            .query("N < 2")["border"]
        )

        # keep target borders
        df_multi_grt = df_transfer_year.loc[
            df_transfer_year["border"].isin(borders.unique())
        ]

        # select one line group by border (one border = 2 lines/2 ZONE)
        # RULES 1: IF no curve id => keep min of NTC_LIMIT_CAPACITY_STATIC
        df_multi_grt.loc[:, "NTC_CURVE_ID"] = df_multi_grt["NTC_CURVE_ID"].astype(
            "string"
        )

        mask = df_multi_grt["NTC_CURVE_ID"].isna()

        df_multi_grt_1 = df_multi_grt[
            mask.groupby(df_multi_grt["border"]).transform("all")
        ]

        # keep row with min
        df_multi_grt_1_min = df_multi_grt_1.loc[
            df_multi_grt_1.groupby("border")["NTC_LIMIT_CAPACITY_STATIC"].idxmin()
        ]

        # dispatch value of "NTC_LIMIT_CAPACITY_STATIC" on columns "SUMMER_HC", "WINTER_HC", "SUMMER_HP", "WINTER_HP"
        cols = ["SUMMER_HC", "WINTER_HC", "SUMMER_HP", "WINTER_HP"]

        df_multi_grt_1_min[cols] = np.tile(
            df_multi_grt_1_min["NTC_LIMIT_CAPACITY_STATIC"].to_numpy()[:, None],
            (1, len(cols)),
        )

        # RULES 2: IF curve id
        # RULES 2.1: one curve id by border
        # select values who are median columns <= to NTC_LIMIT_CAPACITY_STATIC
        df_multi_grt_2_1 = df_multi_grt[
            (mask.groupby(df_multi_grt["border"]).transform("any"))
            & (~mask.groupby(df_multi_grt["border"]).transform("all"))
        ]

        ref_ntc_values = (
            df_multi_grt_2_1.loc[df_multi_grt_2_1["NTC_CURVE_ID"].isna()]
            .groupby("border")["NTC_LIMIT_CAPACITY_STATIC"]
            .first()
        )

        cols = ["SUMMER_HC", "WINTER_HC", "SUMMER_HP", "WINTER_HP"]

        df_multi_one_id = df_multi_grt_2_1.copy()

        df_multi_one_id[cols] = df_multi_one_id[cols].clip(
            upper=df_multi_one_id["border"].map(ref_ntc_values), axis=0
        )

        df_multi_one_id = df_multi_one_id.loc[~df_multi_one_id["NTC_CURVE_ID"].isna()]

        # RULES 2.2 : one curve by line (2 max per border)
        # keep minimal "MEDIAN" value
        mask = (
            df_multi_grt["NTC_CURVE_ID"]
            .notna()
            .groupby(df_multi_grt["border"])
            .transform("all")
        )

        df_multi_grt_2_2 = df_multi_grt[mask]

        df_multi_grt_2_2 = df_multi_grt_2_2.loc[
            df_multi_grt_2_2.groupby("border")["MEDIAN"].idxmin()
        ]

        # concat all df
        frames = [df_multi_grt_1_min, df_multi_one_id, df_multi_grt_2_2]

        result = pd.concat(frames)

        d_df_year[str(iyear)] = result

    # named list of data frames
    return d_df_year
    # endregion

    # export part


def links_columns_output_format(
    data_dict: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    # hamburger dict to one data frame
    # apply new format
    # split into dict with keys as "year"

    if len(data_dict) == 0:
        raise ValueError("No DATA for export")

    df_concat = pd.concat(
        data_dict.values(), keys=data_dict.keys(), names=["key"]
    ).reset_index(level="key")

    # order column with alphabetical
    df_concat["ANTARES"] = sort_code_antares(
        data_frame=df_concat, col_names=["code_source", "code_destination"]
    )

    # structure data with links way
    df_concat["links_way"] = np.where(
        df_concat["border"] == df_concat["ANTARES"],
        "direct",
        "indirect",
    )

    df_direct = df_concat.loc[df_concat["links_way"] == "direct"]
    df_indirect = df_concat.loc[df_concat["links_way"] == "indirect"]

    Col = conf_links.ExportLinksColumnsNames
    df_direct_pegase = pd.DataFrame(
        {
            "key": df_direct["key"],
            Col.NAME.value: df_direct["ANTARES"],
            Col.WINTER_HP_DIRECT_MW.value: df_direct["WINTER_HP"],
            Col.WINTER_HC_DIRECT_MW.value: df_direct["WINTER_HC"],
            Col.SUMMER_HP_DIRECT_MW.value: df_direct["SUMMER_HP"],
            Col.SUMMER_HC_DIRECT_MW.value: df_direct["SUMMER_HC"],
        }
    )

    df_indirect_pegase = pd.DataFrame(
        {
            "key": df_indirect["key"],
            Col.NAME.value: df_indirect["ANTARES"],
            Col.WINTER_HP_INDIRECT_MW.value: df_indirect["WINTER_HP"],
            Col.WINTER_HC_INDIRECT_MW.value: df_indirect["WINTER_HC"],
            Col.SUMMER_HP_INDIRECT_MW.value: df_indirect["SUMMER_HP"],
            Col.SUMMER_HC_INDIRECT_MW.value: df_indirect["SUMMER_HC"],
        }
    )

    # merge direct/indirect by key/ZONE
    df_direct_indirect = pd.merge(
        df_direct_pegase,
        df_indirect_pegase,
        on=["key", "Name"],
        how="inner",
    )

    # concat columns and global values for the export file
    df_static_columns_values = pd.DataFrame(
        {
            Col.FLOWBASED_PERIMETER.value: [False],
            Col.HVDC_DIRECT.value: [pd.NA],
            Col.HVDC_INDIRECT.value: [pd.NA],
            Col.SPECIFIC_TS.value: [False],
            Col.FORCED_OUTAGE_HVAC.value: [False],
        }
    )

    # add static values
    df_direct_indirect["_tmp"] = 1
    df_static_columns_values["_tmp"] = 1

    df_direct_indirect = df_direct_indirect.merge(
        df_static_columns_values, on="_tmp", how="left"
    ).drop(columns="_tmp")

    # order columns to export
    cols = [c.value for c in Col]
    df_direct_indirect = df_direct_indirect.loc[:, ["key"] + cols]

    # convert to dict of DataFrame
    dfs_by_year: dict[str, pd.DataFrame] = {
        str(year): g.drop(columns="key").reset_index(drop=True)
        for year, g in df_direct_indirect.groupby("key")
    }

    return dfs_by_year

# TODO add tests and rename function "links_..."
# new column "ANTARES"
# oder by alphabetical code
def sort_code_antares(
    data_frame: pd.DataFrame, col_names: list[str], separator: str = "-"
) -> pd.Series:
    return pd.Series(np.sort(data_frame[col_names], axis=1).tolist()).str.join(
        separator
    )
