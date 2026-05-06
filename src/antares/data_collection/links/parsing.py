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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias

import pandas as pd

from antares.data_collection.links.constants import (
    CURVE_UID_SPLIT_SYMBOL,
    LINKS_NTC_INDEX_NAME,
    LINKS_NTC_TS_NAME,
    LINKS_TRANSFER_LINKS_NAME,
    NTC_FILTER_STR_VALUE,
    InputNTCsColumns,
    InputNTCsIndexColumns,
    InputTransferLinksColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    add_code_antares_colum,
    filter_based_on_study_scenarios,
    filter_based_on_year_range,
    filter_non_declared_areas,
    parse_input_file,
)

# mapping used for an index file
ZoneId: TypeAlias = str
NtcCurveId: TypeAlias = str
CurveUid: TypeAlias = str

IndexMapping: TypeAlias = dict[ZoneId, dict[NtcCurveId, CurveUid]]


@dataclass
class TimeSeriesMedianValues:
    winter_hc: Any
    winter_hp: Any
    summer_hc: Any
    summer_hp: Any
    median_value: Any


NtcMedianRepartition: TypeAlias = dict[ZoneId, dict[CurveUid, list[TimeSeriesMedianValues]]]


@dataclass(frozen=True)
class InternalMapping:
    index: IndexMapping
    data: NtcMedianRepartition


class LinksParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _parse_transfer_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_TRANSFER_LINKS_NAME, list(InputTransferLinksColumns))

    def _filter_based_on_transfer_type(self, ntc_name_column: str, df: pd.DataFrame) -> pd.DataFrame:
        if ntc_name_column not in df.columns:
            raise ValueError(f"Column {ntc_name_column} not found in the dataframe 'Transfer Links'")
        return df[df[ntc_name_column] == NTC_FILTER_STR_VALUE]

    def _parse_index_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_NTC_INDEX_NAME, list(InputNTCsIndexColumns))

    def _compute_ntc_median_repartition(self, df: pd.DataFrame) -> NtcMedianRepartition:
        # grouped medians by month and hour
        hours = df[InputNTCsColumns.HOUR]
        months = df[InputNTCsColumns.MONTH]

        hour_labels = [self.main_params.get_peak_hour_label(h) for h in hours]
        month_labels = [self.main_params.get_peak_month_label(m) for m in months]

        df_labels = pd.DataFrame({"hour_label": hour_labels, "month_label": month_labels}, index=df.index)

        df_full = pd.concat([df, df_labels], axis=1)
        grouped = df_full.groupby(["month_label", "hour_label"])
        medians = grouped.median()

        # compute median values for each zone and curve
        exclude_columns = list(InputNTCsColumns)
        cols_to_use = df.columns.difference(exclude_columns)

        result: NtcMedianRepartition = {}
        for col in cols_to_use:
            zone_id = col.split(CURVE_UID_SPLIT_SYMBOL)[0]
            median = df[col].median()
            winter_hc_median = medians.loc[("winter", "HC"), col]
            winter_hp_median = medians.loc[("winter", "HP"), col]
            summer_hc_median = medians.loc[("summer", "HC"), col]
            summer_hp_median = medians.loc[("summer", "HP"), col]

            result.setdefault(zone_id, {})[col] = [
                TimeSeriesMedianValues(
                    winter_hc=winter_hc_median,
                    winter_hp=winter_hp_median,
                    summer_hc=summer_hc_median,
                    summer_hp=summer_hp_median,
                    median_value=median,
                )
            ]

        return result

    def _build_links_index_mapping(self, df: pd.DataFrame) -> IndexMapping:
        cols_to_group = [InputNTCsIndexColumns.ZONE.value, InputNTCsIndexColumns.ID.value]
        groups = df.groupby(by=cols_to_group, as_index=False)
        mapping: IndexMapping = {}

        for (zone, ntc_id), grouped_df in groups:
            assert isinstance(zone, ZoneId)
            assert isinstance(ntc_id, NtcCurveId)
            mapping.setdefault(zone, {})[ntc_id] = str(grouped_df[InputNTCsIndexColumns.CURVE_UID])

        return mapping

    def _build_transfer_links_filtered(self, df: pd.DataFrame) -> pd.DataFrame:
        # process transfer links file pre filter
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        df = filter_based_on_study_scenarios(
            df, self.main_params, self.years, InputTransferLinksColumns.STUDY_SCENARIO.value
        )
        df = filter_based_on_year_range(
            df,
            self.years,
            InputTransferLinksColumns.YEAR_VALID_START.value,
            InputTransferLinksColumns.YEAR_VALID_END.value,
        )
        df = self._filter_based_on_transfer_type(InputTransferLinksColumns.TRANSFER_TYPE, df)

        df = add_code_antares_colum(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = add_code_antares_colum(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        return df

    def _build_pegase_dataframe(self, df: pd.DataFrame, year: int, data: IndexMapping) -> pd.DataFrame:
        pass
        # filter on year
        mask = (df[InputTransferLinksColumns.YEAR_VALID_START] <= year) & (
            df[InputTransferLinksColumns.YEAR_VALID_END] >= year
        )
        df = df.loc[mask]

        return 0

    def build_links(self) -> None:
        df = self._parse_transfer_links()
        df = self._build_transfer_links_filtered(df)

        # parsing index file + build mapping index
        links_index_df = self._parse_index_links()
        index_mapping = self._build_links_index_mapping(links_index_df)

        # parsing time series file
        links_ntc_ts_df = pd.read_csv(self.input_folder / LINKS_NTC_TS_NAME)

        # build index of median values
        indexes_ntc_median_repartition = self._compute_ntc_median_repartition(links_ntc_ts_df)

        all_data_indexes = InternalMapping(index=index_mapping, data=indexes_ntc_median_repartition)

        # # treatments for every year
        # index_of_df_pegase: dict[int, pd.DataFrame] = {}
        # for year in self.years:
        #     df_year = self._build_pegase_dataframe(df, year, all_data_indexes)
        #     index_of_df_pegase[year] = df_year
