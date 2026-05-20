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
from typing import TypeAlias

import pandas as pd

from antares.data_collection.constants import ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS
from antares.data_collection.misc.constants import MISC_CATEGORY_NAME, InputMiscColumns
from antares.data_collection.misc.installed_power.constants import OutputMiscPowerColumns
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import ANTARES_CLUSTER_NAME_COLUMN, filter_out_based_on_year

AntaresNodeId: TypeAlias = str
MiscClusterId: TypeAlias = str
CapacityValue: TypeAlias = float
IndexCapacityCluster: TypeAlias = dict[AntaresNodeId, dict[MiscClusterId, CapacityValue]]


class MiscInstalledPowerParser:
    def __init__(self, output_folder: Path, main_params: MainParams, years: list[int]):
        self.output_folder = output_folder
        self.main_params = main_params
        self.years = years

    def _compute_installed_power_year(self, df: pd.DataFrame, year: int) -> IndexCapacityCluster:
        """
        Compute MISC metrics for a given year:
            - sum of capacity installed for each cluster (rounded to MAX_DECIMAL_DIGITS)
        """

        df_year = filter_out_based_on_year(
            df, year, InputMiscColumns.COMMISSIONING_DATE.value, InputMiscColumns.DECOMMISSIONING_DATE_EXPECTED.value
        )

        # Group
        grouped_df = df_year.groupby([ANTARES_NODE_NAME_COLUMN, ANTARES_CLUSTER_NAME_COLUMN], as_index=False)

        output_dict: IndexCapacityCluster = {}
        for (area, cluster), grouped_df_cluster in grouped_df:
            assert isinstance(area, AntaresNodeId)
            assert isinstance(cluster, MiscClusterId)
            output_dict.setdefault(area, {})[cluster] = round(
                grouped_df_cluster[InputMiscColumns.NET_MAX_GEN_CAP].sum(), MAX_DECIMAL_DIGITS
            )

        return output_dict

    def _build_pegase_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        records = []

        for year in self.years:
            for area, cluster_dict in self._compute_installed_power_year(df, year).items():
                for cluster, capacity in cluster_dict.items():
                    records.append((area, cluster, year, capacity))

        df = pd.DataFrame(
            records, columns=[OutputMiscPowerColumns.AREA, OutputMiscPowerColumns.GROUP, "year", "capacity"]
        )

        df_final = df.pivot_table(
            index=[OutputMiscPowerColumns.AREA, OutputMiscPowerColumns.GROUP],
            columns="year",
            values="capacity",
            fill_value=0,
        ).reset_index()

        df_final.columns.name = None

        # Add static values
        df_final[OutputMiscPowerColumns.TO_USE] = 1
        df_final[OutputMiscPowerColumns.CATEGORY] = MISC_CATEGORY_NAME

        return df_final

    def build_misc_installed_power(self, df: pd.DataFrame) -> None:
        self._build_pegase_dataframe(df)
