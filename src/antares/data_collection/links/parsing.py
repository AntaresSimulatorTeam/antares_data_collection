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
from typing import Literal, NamedTuple, TypeAlias

import pandas as pd

from antares.data_collection.links.constants import (
    CURVE_UID_SPLIT_SYMBOL,
    LINKS_NTC_INDEX_NAME,
    LINKS_NTC_TS_NAME,
    LINKS_TRANSFER_LINKS_NAME,
    NTC_FILTER_STR_VALUE,
    ExportLinksColumnsNames,
    InputNTCsColumns,
    InputNTCsIndexColumns,
    InputTransferLinksColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import (
    filter_based_on_study_scenarios,
    filter_based_on_year_range,
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


# @dataclass
# class LinkProfile:
#     # zone_id: ZoneId
#     # antares_border_pair: str
#     technology: list[str]
#     # market_node_pair: str
#     direction: str
#     ntc_curve_id: list[NtcCurveId]
#     ntc_value: list[int]
#     poles: list[float]
#     for_rate: list[float]
#
#
# MarketNodePair: TypeAlias = tuple[str, str]
# InternalLinksProfile: TypeAlias = dict[ZoneId, dict[MarketNodePair, LinkProfile]]


class AggregatedValues(NamedTuple):
    winter_hp: float = 0.0
    winter_hc: float = 0.0
    summer_hp: float = 0.0
    summer_hc: float = 0.0
    reference_capacity: float = 0.0
    hvdc_mw: float = 0.0
    hvdc_nb: int = 0
    hvdc_for: float = 0.0


Direction: TypeAlias = Literal["direct", "indirect"]
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

    def _filter_based_on_transfer_type(self, ntc_name_column: str, df: pd.DataFrame) -> pd.DataFrame:
        if ntc_name_column not in df.columns:
            raise ValueError(f"Column {ntc_name_column} not found in the dataframe 'Transfer Links'")
        return df[df[ntc_name_column] == NTC_FILTER_STR_VALUE]

    def _parse_index_links(self) -> pd.DataFrame:
        return parse_input_file(self.input_folder / LINKS_NTC_INDEX_NAME, list(InputNTCsIndexColumns))

    def _add_links_code_antares_column(self, df: pd.DataFrame, market_node_name_column: str) -> pd.DataFrame:
        if market_node_name_column not in df.columns:
            raise ValueError(f"Column {market_node_name_column} not found in the dataframe 'Transfer Links'")

        node_list = df[market_node_name_column].tolist()
        df[market_node_name_column] = self.main_params.get_links_antares_codes(node_list)
        return df

    def _filter_non_declared_links_areas(self, df: pd.DataFrame, market_node_name_column: str) -> pd.DataFrame:
        """
        Some nodes are not inside RTE study perimeter and therefore not registered inside the main parameters file.
        We don't want to consider them.
        We simply log a message for each area we find in this case
        """
        if market_node_name_column not in df.columns:
            raise ValueError(f"Column {market_node_name_column} not found in the dataframe")

        all_market_nodes = set(df[market_node_name_column])
        missing_nodes = []
        for node in all_market_nodes:
            antares_code = self.main_params.get_links_antares_code(node)
            if not antares_code:
                missing_nodes.append(node)

        if missing_nodes:
            return df[~df[market_node_name_column].isin(missing_nodes)]
        return df

    def _filter_duplicate_market(self, df: pd.DataFrame) -> pd.DataFrame:
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
            assert pd.api.types.is_float_dtype(df[col])

            zone_id = col.split(CURVE_UID_SPLIT_SYMBOL)[0]
            w_hc = dict_medians["winter"]["HC"][col]
            w_hp = dict_medians["winter"]["HP"][col]
            s_hc = dict_medians["summer"]["HC"][col]
            s_hp = dict_medians["summer"]["HP"][col]
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
            # Extraction explicite des valeurs du tuple
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
        df = self._filter_non_declared_links_areas(df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = self._filter_non_declared_links_areas(df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

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

        df = self._add_links_code_antares_column(df, InputTransferLinksColumns.MARKET_ZONE_SOURCE)
        df = self._add_links_code_antares_column(df, InputTransferLinksColumns.MARKET_ZONE_DESTINATION)

        df = self._filter_duplicate_market(df)

        return df

    # def _build_market_node_pair_value(self, market_node_source: str, market_node_destination: str) -> MarketNodePair:
    #     return (market_node_source, market_node_destination)
    #
    # def _get_link_direction(self, market_node_source: str, market_node_destination: str) -> Direction:
    #     return "direct" if market_node_source < market_node_destination else "indirect"
    #
    # def _to_link_profile(
    #     self,
    #     source: str,
    #     destination: str,
    #     df: pd.DataFrame,
    # ) -> LinkProfile:
    #     return LinkProfile(
    #         technology=df[InputTransferLinksColumns.TRANSFER_TECHNOLOGY].tolist(),
    #         direction=self._get_link_direction(source, destination),
    #         ntc_curve_id=df[InputTransferLinksColumns.NTC_CURVE_ID].tolist(),
    #         ntc_value=df[InputTransferLinksColumns.NTC_LIMIT_CAPACITY_STATIC].tolist(),
    #         poles=df[InputTransferLinksColumns.NO_POLES].tolist(),
    #         for_rate=df[InputTransferLinksColumns.FOR].tolist(),
    #     )
    #
    # def _build_links_profile_index(self, df: pd.DataFrame) -> InternalLinksProfile:
    #     cols_to_group = [
    #         InputTransferLinksColumns.ZONE,
    #         InputTransferLinksColumns.MARKET_ZONE_SOURCE,
    #         InputTransferLinksColumns.MARKET_ZONE_DESTINATION,
    #     ]
    #     grouped = df.groupby(cols_to_group)
    #     links_mapping: InternalLinksProfile = {}
    #
    #     for keys, group in grouped:
    #         zone, source, destination = keys
    #         assert isinstance(zone, ZoneId)
    #         assert isinstance(source, str)
    #         assert isinstance(destination, str)
    #         antares_pair = self._build_market_node_pair_value(source, destination)
    #
    #         links_mapping.setdefault(zone, {})[antares_pair] = self._to_link_profile(
    #             source=source,
    #             destination=destination,
    #             df=group,
    #         )
    #
    #     return links_mapping

    # def _select_links_profile(self, index: InternalLinksProfile):
    #     result: InternalLinksProfile = {}
    #     for zone, links_profile in index.items():
    #         # result[zone] = {}
    #         for antares_pair, link_profile in links_profile.items():
    #             if link_profile.ntc_value:
    #                 result[zone][antares_pair] = link_profile
    #     return result

    def _get_profile_values(self, row: pd.Series, mapping: InternalMapping) -> AggregatedValues:
        """For every row/profile we treat cas with NTC curve and NTC static value."""
        tech = row[InputTransferLinksColumns.TRANSFER_TECHNOLOGY]
        curve_id = row[InputTransferLinksColumns.NTC_CURVE_ID]
        static_ntc = row[InputTransferLinksColumns.NTC_LIMIT_CAPACITY_STATIC]
        zone = row[InputTransferLinksColumns.ZONE]

        # HVDC
        is_hvdc = tech == "HVDC"
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
                    hvdc_for=hvdc_for,
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

    def _select_links_profile(self, df: pd.DataFrame, mapping: InternalMapping) -> pd.DataFrame:
        # 1. Grouper par paire de nœuds (triée A-Z) et par GRT (ZoneId)
        # rows = []

        # On normalise d'abord les paires pour identifier les "Direct" vs "Indirect"
        # par rapport à l'ordre alphabétique du couple Name (ex: DE-FR)
        processed_data: CrossGrtIndex = {}

        for _, row in df.iterrows():
            n1, n2 = (
                row[InputTransferLinksColumns.MARKET_ZONE_SOURCE],
                row[InputTransferLinksColumns.MARKET_ZONE_DESTINATION],
            )
            pair = tuple(sorted([n1, n2]))
            direction: Direction = "direct" if n1 < n2 else "indirect"
            zone = row[InputTransferLinksColumns.ZONE]

            vals = self._get_profile_values(row, mapping)

            # Initialisation structures imbriquées
            pair_data = processed_data.setdefault(pair, {})
            grt_data = pair_data.setdefault(zone, {"direct": AggregatedValues(), "indirect": AggregatedValues()})

            # Somme des technologies (HVAC + HVDC) et des market nodes pour le même GRT
            current = grt_data[direction]
            grt_data[direction] = AggregatedValues(
                winter_hp=current.winter_hp + vals.winter_hp,
                winter_hc=current.winter_hc + vals.winter_hc,
                summer_hp=current.summer_hp + vals.summer_hp,
                summer_hc=current.summer_hc + vals.summer_hc,
                reference_capacity=current.reference_capacity + vals.reference_capacity,
                hvdc_mw=current.hvdc_mw + vals.hvdc_mw,
                hvdc_nb=current.hvdc_nb + vals.hvdc_nb,
                hvdc_for=(current.hvdc_for + vals.hvdc_for) / 2 if current.hvdc_nb > 0 else vals.hvdc_for,
            )

        # 2. Application de la règle du Minimum entre GRT
        final_output = []
        for pair, grts in processed_data.items():
            name = f"{pair[0]}-{pair[1]}"

            # Sélection du minimum pour le sens DIRECT
            direct_winner = min(grts.values(), key=lambda x: x["direct"].reference_capacity)["direct"]
            # Sélection du minimum pour le sens INDIRECT
            indirect_winner = min(grts.values(), key=lambda x: x["indirect"].reference_capacity)["indirect"]

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

        # links_profile_indexed = self._build_links_profile_index(df)
        #
        # selected_links_profile = self._select_links_profile(links_profile_indexed)

        df_pegase = self._select_links_profile(df, data)

        # TODO have a pattern
        # profile = normalize(row)
        # profile = merge_technologies(profile)
        # profile = aggregate_nodes(profile)
        # profile = select_min_grt(profile)

        return df_pegase

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
