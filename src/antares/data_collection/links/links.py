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
import os
from pathlib import Path
from typing import Sequence, Hashable

import numpy as np

from antares.data_collection.links import conf_links

import pandas as pd

from antares.data_collection.tools.conf import LocalConfiguration

# Data referential
from antares.data_collection.links.conf_links import (
    ReferentialSheetNames as RefSheetNames,
    LinksColumnsNames,
    LinksExportParameters,
)
from antares.data_collection.links.conf_links import PeakParamsColumnsNames as RefPeak

# Data Links
from antares.data_collection.links.conf_links import (
    NTCS,
    NTCsIndex,
    TransferLinks,
    StudyScenarioColumnsNames,
)

# internal function(s)
from antares.data_collection.tools import tools
from antares.data_collection.tools.tools import create_xlsx_workbook, edit_xlsx_workbook


def links_data_management(conf_input: LocalConfiguration) -> dict[str, pd.DataFrame]:
    """
    Manage links data (Transfer capacity, NTC TS + INDEX).

    The following treatments are applied:
    - NTC TS + INDEX: median grouped by HP/HC and Winter/Summer.
    - Transfer capacity pre-treatments: global filters
      `TRANSFER_TYPE = NTC` and `TRANSFER_TECHNOLOGY = HVAC`.
    - Yearly treatments: multi GRT processing.

    Parameters:
        conf_input: Configuration object.

    Returns:
        A dictionary mapping years to pandas DataFrames. If there is no data
        for a specific year, the corresponding DataFrame is empty.
    """

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
        df_ts_ntc,
        ref_months,
        left_on=NTCS.MONTH,
        right_on=RefPeak.MONTH.value,
        how="left",
    )
    df_ts_ntc = df_ts_ntc.drop(
        columns=[
            RefPeak.HOUR.value,
            RefPeak.MONTH.value,
            NTCS.MONTH,
            NTCS.DAY,
            NTCS.HOUR,
        ]
    )

    # compute median hours/saison
    df_median_grouped = df_ts_ntc.groupby(
        by=[RefPeak.PERIOD_HOUR.value, RefPeak.PERIOD_MONTH.value], as_index=False
    ).median()
    series_median = df_ts_ntc.median(numeric_only=True)

    # retreatment + pivot to merge
    df_median_tot = pd.DataFrame(
        {NTCsIndex.CURVE_UID: series_median.index, "MEDIAN": series_median.values}
    )
    df_median_grouped["colname"] = (
        df_median_grouped[RefPeak.PERIOD_MONTH.value]
        .astype(str)
        .str.cat(df_median_grouped[RefPeak.PERIOD_HOUR.value].astype(str), sep="_")
    )
    df_median_grouped["colname"] = df_median_grouped["colname"].str.upper()
    df_median_grouped = df_median_grouped.drop(
        columns=[RefPeak.PERIOD_MONTH.value, RefPeak.PERIOD_HOUR.value]
    )

    df_pivot = (
        df_median_grouped.set_index("colname")
        .T.reset_index()
        .rename(columns={"index": NTCsIndex.CURVE_UID})
    )

    # df with all computed medians by curve_id
    df_ts_median = pd.merge(df_pivot, df_median_tot, how="left")

    # merge median with ntc index
    df_ts_ntc_index = (
        results[conf_links_files.NTC_INDEX]
        .copy()
        .drop(columns=[NTCsIndex.LABEL, NTCsIndex.COUNT])
    )
    df_ts_ntc_index = pd.merge(
        df_ts_ntc_index, df_ts_median, on=NTCsIndex.CURVE_UID, how="left"
    )
    # endregion

    # region
    # Transfer capacity
    # global filter `TRANSFER_TYPE` = NTC + `TRANSFER_TECHNOLOGY` = HVAC
    df_transfer = results[conf_links_files.TRANSFER_LINKS].copy()

    # select only usefully columns
    list_col_to_keep = [
        TransferLinks.ZONE,
        TransferLinks.MARKET_ZONE_SOURCE,
        TransferLinks.MARKET_ZONE_DESTINATION,
        TransferLinks.TRANSFER_TYPE,
        TransferLinks.STUDY_SCENARIO,
        TransferLinks.YEAR_VALID_START,
        TransferLinks.YEAR_VALID_END,
        TransferLinks.TRANSFER_TECHNOLOGY,
        TransferLinks.NTC_LIMIT_CAPACITY_STATIC,
        TransferLinks.NTC_CURVE_ID,
    ]

    df_transfer = df_transfer[list_col_to_keep]

    df_transfer = df_transfer.loc[
        (df_transfer[TransferLinks.TRANSFER_TYPE] == "NTC")
        & (df_transfer[TransferLinks.TRANSFER_TECHNOLOGY] == "HVAC")
    ]

    # merge data with computed median
    # NOTE:
    # mypy cannot infer that list[StrEnum] satisfies Sequence[Hashable]
    # for pandas.merge(left_on/right_on). We must pre-type the sequences.
    left_keys: Sequence[Hashable] = [
        TransferLinks.ZONE,
        TransferLinks.NTC_CURVE_ID,
    ]

    right_keys: Sequence[Hashable] = [
        NTCsIndex.ZONE,
        NTCsIndex.ID,
    ]

    df_transfer = pd.merge(
        df_transfer,
        df_ts_ntc_index,
        left_on=left_keys,
        right_on=right_keys,
        how="left",
    ).drop(columns=[NTCsIndex.ID, NTCsIndex.CURVE_UID])

    # merge column 'code_antares' :
    # for market zone source and market zone destination
    ref_country_links = pd.read_excel(
        conf_input.data_references_path, sheet_name=RefSheetNames.LINKS.value
    )

    # source
    df_transfer = pd.merge(
        df_transfer,
        ref_country_links,
        left_on=TransferLinks.MARKET_ZONE_SOURCE,
        right_on=LinksColumnsNames.MARKET_NODE.value,
        how="left",
    )
    df_transfer = df_transfer.drop(columns=[LinksColumnsNames.MARKET_NODE.value])
    df_transfer = df_transfer.rename(
        columns={LinksColumnsNames.CODE_ANTARES.value: "code_source"}
    )

    # destination
    df_transfer = pd.merge(
        df_transfer,
        ref_country_links,
        left_on=TransferLinks.MARKET_ZONE_DESTINATION,
        right_on=LinksColumnsNames.MARKET_NODE.value,
        how="left",
    )
    df_transfer = df_transfer.drop(columns=[LinksColumnsNames.MARKET_NODE.value])
    df_transfer = df_transfer.rename(
        columns={LinksColumnsNames.CODE_ANTARES.value: "code_destination"}
    )

    # delete row with NAN
    df_transfer = df_transfer.dropna(subset=["code_source", "code_destination"])

    # ADD new column "border" to combine code source + destination
    df_transfer["border"] = (
        df_transfer["code_source"] + "-" + df_transfer["code_destination"]
    )

    # treatment for calendar year
    # filter with scenario and calendar year
    year_param = conf_input.calendar_year
    ref_scenario = pd.read_excel(
        conf_input.data_references_path, sheet_name=RefSheetNames.STUDY_SCENARIO.value
    )

    d_df_year = {}
    for iyear in year_param:
        # filter scenario
        scenario_values = ref_scenario.loc[
            ref_scenario[StudyScenarioColumnsNames.YEAR.value].isin([iyear])
        ].STUDY_SCENARIO.item()

        df_transfer_year = tools.scenario_filter(
            df_input=df_transfer, filter_params=scenario_values
        )

        # filter by year
        df_transfer_year = df_transfer_year.loc[
            (df_transfer_year[TransferLinks.YEAR_VALID_START] <= iyear)
            & (df_transfer_year[TransferLinks.YEAR_VALID_END] >= iyear)
        ]

        # drop columns with market_zon code + year columns
        df_transfer_year = df_transfer_year.drop(
            columns=[
                TransferLinks.MARKET_ZONE_SOURCE,
                TransferLinks.MARKET_ZONE_DESTINATION,
            ]
        ).drop(columns=[TransferLinks.YEAR_VALID_START, TransferLinks.YEAR_VALID_END])

        ##
        # multi GRT treatment
        ##

        # identify multi grt only
        borders = (
            df_transfer_year.groupby([TransferLinks.ZONE, "border"])
            .size()
            .reset_index(name="N")
            .query("N < 2")["border"]
        )

        # NO multi grt (other)
        df_no_multi_grt = df_transfer_year.loc[
            ~df_transfer_year["border"].isin(borders.unique())
        ]

        # aggregation one more time for country with multi market zone
        key_cols = [
            TransferLinks.ZONE.value,
            TransferLinks.TRANSFER_TYPE.value,
            TransferLinks.STUDY_SCENARIO.value,
            TransferLinks.TRANSFER_TECHNOLOGY.value,
            TransferLinks.NTC_CURVE_ID.value,
            "border",
            "code_destination",
            "code_source",
        ]

        # group by "key_cols" and keep NA with MEDIAN WINTER/SUMMER with parameter `min_count=1`
        df_no_multi_grt = df_no_multi_grt.groupby(
            key_cols, dropna=False, as_index=False
        ).sum(numeric_only=True, min_count=1)

        # NOTE : df contains MONO GRT too but not a pb for the treatments

        # keep target borders multi GRT
        df_multi_grt = df_transfer_year.loc[
            df_transfer_year["border"].isin(borders.unique())
        ]

        # combine/integrate reaggregate grt with multi grt
        frames = [df_multi_grt, df_no_multi_grt]

        df_multi_grt = pd.concat(frames).reset_index(drop=True)

        # select one line group by border (one border = 2 lines/2 ZONE)
        # RULES 1: IF no curve id => keep min of NTC_LIMIT_CAPACITY_STATIC
        df_multi_grt.loc[:, TransferLinks.NTC_CURVE_ID] = df_multi_grt[
            TransferLinks.NTC_CURVE_ID
        ].astype("string")

        mask = df_multi_grt[TransferLinks.NTC_CURVE_ID].isna()

        df_multi_grt_1 = df_multi_grt[
            mask.groupby(df_multi_grt["border"]).transform("all")
        ]

        # keep row with min
        df_multi_grt_1_min = df_multi_grt_1.loc[
            df_multi_grt_1.groupby("border")[
                TransferLinks.NTC_LIMIT_CAPACITY_STATIC
            ].idxmin()
        ]

        # dispatch value of "NTC_LIMIT_CAPACITY_STATIC" on columns "SUMMER_HC", "WINTER_HC", "SUMMER_HP", "WINTER_HP"
        cols = ["SUMMER_HC", "WINTER_HC", "SUMMER_HP", "WINTER_HP"]

        df_multi_grt_1_min[cols] = np.tile(
            df_multi_grt_1_min[TransferLinks.NTC_LIMIT_CAPACITY_STATIC].to_numpy()[
                :, None
            ],
            (1, len(cols)),
        )

        # RULES 2: IF curve id
        # RULES 2.1: one curve id by border
        # select values who are median columns <= to NTC_LIMIT_CAPACITY_STATIC
        df_multi_grt_2_1 = df_multi_grt[
            (mask.groupby(df_multi_grt["border"]).transform("any"))
            & (~mask.groupby(df_multi_grt["border"]).transform("all"))
        ]

        df_multi_one_id = df_multi_grt_2_1.loc[
            ~df_multi_grt_2_1[TransferLinks.NTC_CURVE_ID].isna()
        ]

        # RULES 2.2 : one curve by line (2 max per border)
        # keep minimal "MEDIAN" value
        mask = (
            df_multi_grt[TransferLinks.NTC_CURVE_ID]
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

        # build named dictionary
        d_df_year[str(iyear)] = result

    # named list of data frames
    return d_df_year
    # endregion

    # export part


def links_manage_output_format(
    data_dict: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    # hamburger dict to one data frame
    # apply new format
    # split into dict with keys as "year"

    if len(data_dict) == 0:
        raise ValueError("No DATA for export")

    # concat and a create a col with dict.key() then drop index
    df_concat = (
        pd.concat(data_dict.values(), keys=data_dict.keys(), names=["key"])
        .reset_index(level=0)
        .reset_index(drop=True)
    )

    # order column with alphabetical
    df_concat["ANTARES"] = links_sort_borders_code(
        data_frame=df_concat, col_names=["code_source", "code_destination"]
    )

    # structure data with links way
    df_concat["links_way"] = np.where(
        df_concat["border"] == df_concat["ANTARES"],
        "direct",
        "indirect",
    )

    # delete row with source==destination
    df_concat = df_concat.loc[df_concat["code_source"] != df_concat["code_destination"]]

    # mapping columns for df direct/indirect
    df_direct = df_concat.loc[df_concat["links_way"] == "direct"]
    df_indirect = df_concat.loc[df_concat["links_way"] == "indirect"]

    export_columns = conf_links.ExportLinksColumnsNames
    df_direct_pegase = pd.DataFrame(
        {
            "key": df_direct["key"],
            export_columns.NAME.value: df_direct["ANTARES"],
            export_columns.WINTER_HP_DIRECT_MW.value: df_direct["WINTER_HP"],
            export_columns.WINTER_HC_DIRECT_MW.value: df_direct["WINTER_HC"],
            export_columns.SUMMER_HP_DIRECT_MW.value: df_direct["SUMMER_HP"],
            export_columns.SUMMER_HC_DIRECT_MW.value: df_direct["SUMMER_HC"],
        }
    )

    df_indirect_pegase = pd.DataFrame(
        {
            "key": df_indirect["key"],
            export_columns.NAME.value: df_indirect["ANTARES"],
            export_columns.WINTER_HP_INDIRECT_MW.value: df_indirect["WINTER_HP"],
            export_columns.WINTER_HC_INDIRECT_MW.value: df_indirect["WINTER_HC"],
            export_columns.SUMMER_HP_INDIRECT_MW.value: df_indirect["SUMMER_HP"],
            export_columns.SUMMER_HC_INDIRECT_MW.value: df_indirect["SUMMER_HC"],
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
            export_columns.FLOWBASED_PERIMETER.value: [False],
            export_columns.HVDC.value: [False],
            export_columns.SPECIFIC_TS.value: [False],
            export_columns.FORCED_OUTAGE_HVAC.value: [False],
        }
    )

    # add static values
    df_direct_indirect["_tmp"] = 1
    df_static_columns_values["_tmp"] = 1

    df_direct_indirect = df_direct_indirect.merge(
        df_static_columns_values, on="_tmp", how="left"
    ).drop(columns="_tmp")

    # order columns to export
    cols = [c.value for c in export_columns]
    df_direct_indirect = df_direct_indirect.loc[:, ["key"] + cols]

    # alphabetical order for border by year
    df_direct_indirect = df_direct_indirect.sort_values(by=["key", "Name"])

    # convert to dict of DataFrame
    dfs_by_year: dict[str, pd.DataFrame] = {
        str(year): g.drop(columns="key").reset_index(drop=True)
        for year, g in df_direct_indirect.groupby("key")
    }

    return dfs_by_year


def links_manage_export(
    dict_of_df: dict[str, pd.DataFrame],
    root_dir_export: Path,
    links_dir: list[str] = ["link"],
    scenario_name: str | None = None,
) -> None:
    if len(dict_of_df) == 0:
        raise ValueError("No DATA to export")
    if not root_dir_export.exists():
        raise ValueError(f"Path of root directory {root_dir_export} does not exist")

    # create dir from root dir
    links_specific_dir = Path(*links_dir)
    links_export_path_dir = root_dir_export / links_specific_dir
    os.makedirs(links_export_path_dir, exist_ok=True)

    if scenario_name is not None:
        workbook_name = f"links_{scenario_name}"

    # export every element of the dictionary
    # one year data result by sheet + one sheet "parameters" at first

    # preparation of data of first sheet "parameters"
    def transform_year_to_straddling_year(year_list: list[int]) -> list[str]:
        result_list = []
        for year in year_list:
            result_list.append(str(year - 1) + "-" + str(year))
        return result_list

    years_int: list[int] = [int(k) for k in dict_of_df.keys()]
    all_straddling_years = transform_year_to_straddling_year(years_int)

    df_parameters = pd.DataFrame(
        {
            "year": all_straddling_years,
            LinksExportParameters.HURDLE_COSTS.label: LinksExportParameters.HURDLE_COSTS.default,
        }
    )

    df_parameters_out = pd.DataFrame(
        data=[df_parameters[LinksExportParameters.HURDLE_COSTS.label].values],
        columns=df_parameters["year"],
        index=[LinksExportParameters.HURDLE_COSTS.label],
    )

    df_parameters_out.columns.name = None

    # write file (with index=True especially for this sheet only)
    create_xlsx_workbook(
        path_dir=links_export_path_dir,
        workbook_name=workbook_name,
        sheet_name="parameters",
        data_df=df_parameters_out,
        index=True,
    )

    for year in dict_of_df.keys():
        # the second sheet is for data
        year_param = [str(int(year) - 1), year]
        parameter_col_name = str(year_param[0] + "-" + year_param[1])

        edit_xlsx_workbook(
            path_file=links_export_path_dir / f"{workbook_name}.xlsx",
            sheet_name=parameter_col_name,
            data_df=dict_of_df[year],
        )


# main function to process links files
def create_links_outputs(links_conf_input: LocalConfiguration) -> None:
    """
    Processes input configuration, manages links data, formats data frames,
    and handles export operations.

    Parameters:
        links_conf_input:
            Input configuration used for processing links and exports.

    Returns:
        A new Excel workbook named "links_{scenario_name}.xlsx", created in the root directory of the PEMMDB gateway.
    """
    # data management with specific links files
    dict_managed = links_data_management(conf_input=links_conf_input)

    # manage formats of data frames for export
    dict_of_formated_df = links_manage_output_format(data_dict=dict_managed)

    # exports part
    links_manage_export(
        dict_of_df=dict_of_formated_df,
        root_dir_export=links_conf_input.export_path,
        scenario_name=links_conf_input.scenario_name,
    )


# TODO add tests
# new column "ANTARES"
# oder by alphabetical code
def links_sort_borders_code(
    data_frame: pd.DataFrame, col_names: list[str], separator: str = "-"
) -> pd.Series:
    return pd.Series(np.sort(data_frame[col_names], axis=1).tolist()).str.join(
        separator
    )
