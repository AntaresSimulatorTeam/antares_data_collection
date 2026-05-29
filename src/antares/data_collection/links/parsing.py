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
from statistics import mean
from typing import NamedTuple, TypeAlias

import numpy as np
import pandas as pd

from antares.data_collection.links.constants import (
    CURVE_UID_SPLIT_SYMBOL,
    FIRST_SHEET_NAME,
    HOUR_OFFPEAK,
    HOUR_PEAK,
    HURDLE_COSTS_NAME,
    HURDLE_COSTS_VALUE,
    HVDC_NAME_TECHNOLOGY,
    LINKS_CLUSTER_FOLDER,
    LINKS_NTC_INDEX_NAME,
    LINKS_NTC_TS_NAME,
    LINKS_OUTPUT_NAME_FILE,
    LINKS_TRANSFER_LINKS_NAME,
    NTC_FILTER_STR_VALUE,
    SUMMER_SEASON,
    WINTER_SEASON,
    Direction,
    ExportLinksColumnsNames,
    InputNTCsColumns,
    InputNTCsIndexColumns,
    InputTransferLinksColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_based_on_study_scenarios,
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
    winter_hc: float
    winter_hp: float
    summer_hc: float
    summer_hp: float
    median_value: float


NtcMedianRepartition: TypeAlias = dict[ZoneId, dict[CurveUid, list[TimeSeriesMedianValues]]]


@dataclass(frozen=True)
class InternalMapping:
    index: IndexMapping
    data: NtcMedianRepartition


class AggregatedValues(NamedTuple):
    winter_hp: float = 0.0
    winter_hc: float = 0.0
    summer_hp: float = 0.0
    summer_hc: float = 0.0
    reference_capacity: float = 0.0
    hvdc_mw: float = 0.0
    hvdc_nb: int = 0
    hvdc_for: float = 0.0
    has_curve: bool = False

    @property
    def selection_priority(self) -> tuple[int, int, float, float]:
        is_hvdc = self.hvdc_nb > 0
        is_na_hvdc = is_hvdc and pd.isna(self.hvdc_for)

        hvdc_for_priority = self.hvdc_for if pd.notna(self.hvdc_for) else float("inf")

        return (
            1 if is_na_hvdc else 0,  # exclude NA only for HVDC
            0 if self.has_curve else 1,  # curve first
            self.reference_capacity,  # value capacity curve or NTC
            hvdc_for_priority,  # value to discriminate
        )


# index by "border" pair (sorted), tuple(sorted([node1, node2])) -> "FR-IT"
CrossGrtIndex: TypeAlias = dict[tuple[str, str], dict[ZoneId, dict[Direction, AggregatedValues]]]


class LinksParser:
    def __init__(self, input_folder: Path, output_folder: Path, main_params: MainParams, years: list[int]):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _parse_transfer_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_TRANSFER_LINKS_NAME, list(InputTransferLinksColumns))

    def _filter_based_on_transfer_type(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[InputTransferLinksColumns.TRANSFER_TYPE] == NTC_FILTER_STR_VALUE]

    def _parse_index_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_NTC_INDEX_NAME, list(InputNTCsIndexColumns))

    def _filter_based_on_year_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep rows where at least one year in `years` satisfies:
        start_column <= year <= end_column
        """
        years = self.years

        # Create a mask that is True if any of the requested years are within the range
        mask = pd.Series(False, index=df.index)
        for year in years:
            mask |= (df[InputTransferLinksColumns.YEAR_VALID_START] <= year) & (
                df[InputTransferLinksColumns.YEAR_VALID_END] >= year
            )

        df = df[mask]

        if df.empty:
            raise ValueError(
                f"No input data matched the given years {years} in range {InputTransferLinksColumns.YEAR_VALID_START} - {InputTransferLinksColumns.YEAR_VALID_END}"
            )

        return df

    def _add_links_code_antares_column(self, df: pd.DataFrame, market_node_name_column: str) -> pd.DataFrame:
        node_list = df[market_node_name_column].tolist()
        df[market_node_name_column] = self.main_params.get_antares_codes(node_list)
        return df

    def _filter_duplicate_market_zone(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter row if the market source and destination are the same.
        """
        return df[
            ~(df[InputTransferLinksColumns.MARKET_ZONE_SOURCE] == df[InputTransferLinksColumns.MARKET_ZONE_DESTINATION])
        ]

    def _compute_ntc_median_repartition(self, df: pd.DataFrame) -> NtcMedianRepartition:
        # Identify data columns (excluding technical columns)
        exclude_columns = list(InputNTCsColumns)
        cols_to_use = df.columns.difference(exclude_columns)

        # Compute Grouped Medians (HC/HP/Summer/Winter)
        hours = df[InputNTCsColumns.HOUR]
        months = df[InputNTCsColumns.MONTH]

        hour_labels = [self.main_params.get_peak_hour_label(h) for h in hours]
        month_labels = [self.main_params.get_peak_month_label(m) for m in months]

        df_labels = pd.DataFrame({"hour_label": hour_labels, "month_label": month_labels}, index=df.index)
        df_full = pd.concat([df, df_labels], axis=1)

        # Calculate medians grouped by labels
        grouped = df_full.groupby(["month_label", "hour_label"], as_index=False)

        dict_medians: dict[str, dict[str, dict[str, float]]] = {}
        for (month, hour), group in grouped:
            assert isinstance(month, str)
            assert isinstance(hour, str)

            medians = group[cols_to_use].median(numeric_only=True).to_dict()

            typed_medians: dict[str, float] = {}

            for col, value in medians.items():
                assert isinstance(col, str)

                if pd.notna(value):
                    typed_medians[col] = float(value)

            dict_medians.setdefault(month, {})[hour] = typed_medians

        # Build the final NtcMedianRepartition using direct lookups and asserts
        result: NtcMedianRepartition = {}

        for col in cols_to_use:
            zone_id = col.split(CURVE_UID_SPLIT_SYMBOL)[0]
            w_hc = dict_medians[WINTER_SEASON][HOUR_OFFPEAK][col]
            w_hp = dict_medians[WINTER_SEASON][HOUR_PEAK][col]
            s_hc = dict_medians[SUMMER_SEASON][HOUR_OFFPEAK][col]
            s_hp = dict_medians[SUMMER_SEASON][HOUR_PEAK][col]
            g_median = df[col].median()

            # Create the dataclass instance
            median_stats = TimeSeriesMedianValues(
                winter_hc=w_hc, winter_hp=w_hp, summer_hc=s_hc, summer_hp=s_hp, median_value=g_median
            )

            # Assign to the nested mapping
            result.setdefault(zone_id, {})[col] = [median_stats]

        return result

    def _build_links_index_mapping(self, df: pd.DataFrame) -> IndexMapping:
        mapping: IndexMapping = {}
        cols = [InputNTCsIndexColumns.ZONE.value, InputNTCsIndexColumns.ID.value, InputNTCsIndexColumns.CURVE_UID.value]

        for row in df[cols].itertuples(index=False):
            zone = row[0]
            ntc_id = row[1]
            curve_uid = row[2]

            assert isinstance(zone, ZoneId)
            assert isinstance(ntc_id, NtcCurveId)
            assert isinstance(curve_uid, CurveUid)

            mapping.setdefault(zone, {})[ntc_id] = curve_uid

        return mapping

    def _build_transfer_links_filtered(self, df: pd.DataFrame) -> pd.DataFrame:
        # process transfer links file pre filter
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = filter_non_declared_areas(self.main_params, df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        df = filter_based_on_study_scenarios(
            df, self.main_params, self.years, InputTransferLinksColumns.STUDY_SCENARIO.value
        )
        df = self._filter_based_on_year_range(df)
        df = self._filter_based_on_transfer_type(df)

        df = self._add_links_code_antares_column(df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = self._add_links_code_antares_column(df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        df = self._filter_duplicate_market_zone(df)

        return df

    def _get_profile_values(self, row: pd.Series, mapping: InternalMapping) -> AggregatedValues:
        """For every row/profile we treat cas with NTC curve and NTC static value."""
        tech = row[InputTransferLinksColumns.TRANSFER_TECHNOLOGY]
        curve_id = row[InputTransferLinksColumns.NTC_CURVE_ID]
        static_ntc = row[InputTransferLinksColumns.NTC_LIMIT_CAPACITY_STATIC]
        zone = row[InputTransferLinksColumns.ZONE]

        # HVDC
        is_hvdc = tech == HVDC_NAME_TECHNOLOGY
        hvdc_nb = int(row[InputTransferLinksColumns.NO_POLES]) if is_hvdc else 0
        hvdc_for = float(row[InputTransferLinksColumns.FOR]) if is_hvdc else 0.0

        # CASE: curve (priority)
        if pd.notna(curve_id) and curve_id in mapping.index.get(zone, {}):
            curve_uid = mapping.index[zone][curve_id]
            stats_list = mapping.data.get(zone, {}).get(curve_uid, [])
            if stats_list:
                stats = stats_list[0]
                val = AggregatedValues(
                    winter_hp=stats.winter_hp,
                    winter_hc=stats.winter_hc,
                    summer_hp=stats.summer_hp,
                    summer_hc=stats.summer_hc,
                    reference_capacity=stats.median_value,
                    hvdc_mw=stats.median_value if is_hvdc else 0.0,
                    hvdc_nb=hvdc_nb,
                    hvdc_for=hvdc_for,  # if pd.notna(hvdc_for) else 0.0,
                    has_curve=True,
                )
                return val

        # CASE (only static value)
        val_static = float(static_ntc) if pd.notna(static_ntc) else 0.0
        return AggregatedValues(
            winter_hp=val_static,
            winter_hc=val_static,
            summer_hp=val_static,
            summer_hc=val_static,
            reference_capacity=val_static,
            hvdc_mw=val_static if is_hvdc else 0.0,
            hvdc_nb=hvdc_nb,
            hvdc_for=hvdc_for,
        )

    def _mean_strict_positive(self, x: list[int | float]) -> float:
        positive = [v for v in x if v > 0]
        if len(positive) == 0:
            return 0.0
        return mean(positive)

    def _select_links_profile(self, df: pd.DataFrame, mapping: InternalMapping) -> pd.DataFrame:
        # 1. Build an index by pair of nodes (source/destination)
        # Identify direction: "Direct" vs "Indirect" (alphabetical order)
        processed_data: CrossGrtIndex = {}

        for _, row in df.iterrows():
            n1, n2 = (
                row[InputTransferLinksColumns.MARKET_ZONE_SOURCE],
                row[InputTransferLinksColumns.MARKET_ZONE_DESTINATION],
            )
            pair = tuple(sorted([n1, n2]))
            direction = Direction.DIRECT if n1 < n2 else Direction.INDIRECT
            zone = row[InputTransferLinksColumns.ZONE]

            aggregated_values = self._get_profile_values(row, mapping)

            # Init structure
            pair_data = processed_data.setdefault(pair, {})
            grt_data = pair_data.setdefault(
                zone, {Direction.DIRECT: AggregatedValues(), Direction.INDIRECT: AggregatedValues()}
            )

            # Same GRT can contain different "technology" (hvac/hvdc)
            # Same profile (pair/zone/direction) are summed
            current = grt_data[direction]
            grt_data[direction] = AggregatedValues(
                winter_hp=current.winter_hp + aggregated_values.winter_hp,
                winter_hc=current.winter_hc + aggregated_values.winter_hc,
                summer_hp=current.summer_hp + aggregated_values.summer_hp,
                summer_hc=current.summer_hc + aggregated_values.summer_hc,
                reference_capacity=current.reference_capacity + aggregated_values.reference_capacity,
                hvdc_mw=current.hvdc_mw + aggregated_values.hvdc_mw,
                hvdc_nb=current.hvdc_nb + aggregated_values.hvdc_nb,
                hvdc_for=np.nan
                if (current.hvdc_for == 0) & pd.isna(aggregated_values.hvdc_for)
                else self._mean_strict_positive(
                    [current.hvdc_for, aggregated_values.hvdc_for]
                ),  # available with nan value
                has_curve=current.has_curve or aggregated_values.has_curve,
            )

        # 2. Selection with minimum values (by NTC Capacity or by median value)
        # Select by GRT with same direction (row selection)
        final_output = []
        for pair, grts_aggregated in processed_data.items():
            name = f"{pair[0]}-{pair[1]}"

            # DIRECT minimum Selection
            direct_winner = min(grts_aggregated.values(), key=lambda x: x[Direction.DIRECT].selection_priority)[
                Direction.DIRECT
            ]

            # INDIRECT minimum selection
            indirect_winner = min(grts_aggregated.values(), key=lambda x: x[Direction.INDIRECT].selection_priority)[
                Direction.INDIRECT
            ]

            final_output.append(
                {
                    ExportLinksColumnsNames.NAME: name,
                    ExportLinksColumnsNames.WINTER_HP_DIRECT_MW: direct_winner.winter_hp,
                    ExportLinksColumnsNames.WINTER_HP_INDIRECT_MW: indirect_winner.winter_hp,
                    ExportLinksColumnsNames.WINTER_HC_DIRECT_MW: direct_winner.winter_hc,
                    ExportLinksColumnsNames.WINTER_HC_INDIRECT_MW: indirect_winner.winter_hc,
                    ExportLinksColumnsNames.SUMMER_HP_DIRECT_MW: direct_winner.summer_hp,
                    ExportLinksColumnsNames.SUMMER_HP_INDIRECT_MW: indirect_winner.summer_hp,
                    ExportLinksColumnsNames.SUMMER_HC_DIRECT_MW: direct_winner.summer_hc,
                    ExportLinksColumnsNames.SUMMER_HC_INDIRECT_MW: indirect_winner.summer_hc,
                    ExportLinksColumnsNames.FLOWBASED_PERIMETER: False,
                    ExportLinksColumnsNames.HVDC_DIRECT: direct_winner.hvdc_mw,
                    ExportLinksColumnsNames.HVDC_INDIRECT: indirect_winner.hvdc_mw,
                    ExportLinksColumnsNames.HVDC_NB_DIRECT: direct_winner.hvdc_nb,
                    ExportLinksColumnsNames.HVDC_NB_INDIRECT: indirect_winner.hvdc_nb,
                    ExportLinksColumnsNames.HVDC_FOR_DIRECT: direct_winner.hvdc_for,
                    ExportLinksColumnsNames.HVDC_FOR_INDIRECT: indirect_winner.hvdc_for,
                }
            )

        return pd.DataFrame(final_output).sort_values(by=ExportLinksColumnsNames.NAME)

    def _build_pegase_dataframe(self, df: pd.DataFrame, year: int, data: InternalMapping) -> pd.DataFrame:
        # filter on year
        mask = (df[InputTransferLinksColumns.YEAR_VALID_START] <= year) & (
            df[InputTransferLinksColumns.YEAR_VALID_END] >= year
        )
        df = df.loc[mask]

        # profile processing
        df_pegase = self._select_links_profile(df, data)
        return df_pegase

    def _transform_year_to_straddling_year(self) -> list[str]:
        result_list = []
        for year in self.years:
            result_list.append(str(year - 1) + "-" + str(year))
        return result_list

    def _export_links_to_excel(self, index_of_df_year: dict[int, pd.DataFrame]) -> None:
        parent_dir = self.output_folder / LINKS_CLUSTER_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / LINKS_OUTPUT_NAME_FILE

        # create the first sheet "parameters" (business format)
        all_straddling_years = self._transform_year_to_straddling_year()

        df_parameters_out = pd.DataFrame(
            columns=all_straddling_years,
            data=[[HURDLE_COSTS_VALUE] * len(all_straddling_years)],
            index=[HURDLE_COSTS_NAME],
        )

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # first sheet
            df_parameters_out.to_excel(
                writer,
                sheet_name=FIRST_SHEET_NAME,
            )

            # yearly sheets
            for year, df in index_of_df_year.items():
                sheet_name = f"{year - 1}-{year}"

                df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                )

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

        # treatments for every year
        index_of_df_pegase: dict[int, pd.DataFrame] = {}
        for year in self.years:
            df_year = self._build_pegase_dataframe(df, year, all_data_indexes)
            index_of_df_pegase[year] = df_year

        self._export_links_to_excel(index_of_df_pegase)
