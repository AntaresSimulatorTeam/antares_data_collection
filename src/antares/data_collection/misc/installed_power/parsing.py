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

from antares.data_collection.constants import ANTARES_CLUSTER_NAME_COLUMN, ANTARES_NODE_NAME_COLUMN, MAX_DECIMAL_DIGITS
from antares.data_collection.misc.constants import InputMiscColumns
from antares.data_collection.misc.installed_power.constants import (
    MISC_CATEGORY_NAME,
    MISC_INSTALL_POWER_FOLDER,
    MISC_INSTALL_POWER_NAME_FILE,
    OutputMiscPowerColumns,
)
from antares.data_collection.referential_data.main_params import MainParams
from antares.data_collection.utils import filter_out_based_on_year

AntaresNodeId: TypeAlias = str
PemmdbPlantTypeId: TypeAlias = str
ClusterBpId: TypeAlias = str
CapacityValue: TypeAlias = float
IndexCapacityCluster: TypeAlias = dict[AntaresNodeId, dict[tuple[PemmdbPlantTypeId, ClusterBpId], CapacityValue]]


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

        # Group (specific to misc, group by pemmdb cluster+cluster bp due to export format)
        grouped_df = df_year.groupby(
            [ANTARES_NODE_NAME_COLUMN, InputMiscColumns.PEMMDB_PLANT_TYPE, ANTARES_CLUSTER_NAME_COLUMN], as_index=False
        )

        output_dict: IndexCapacityCluster = {}
        for (area, plant_type_id, clusterbp_id), grouped_df_cluster in grouped_df:
            assert isinstance(area, AntaresNodeId)
            assert isinstance(plant_type_id, PemmdbPlantTypeId)
            assert isinstance(clusterbp_id, ClusterBpId)
            output_dict.setdefault(area, {})[(plant_type_id, clusterbp_id)] = round(
                grouped_df_cluster[InputMiscColumns.NET_MAX_GEN_CAP].sum(), MAX_DECIMAL_DIGITS
            )

        return output_dict

    def _build_pegase_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        records = []

        for year in self.years:
            for area, cluster_dict in self._compute_installed_power_year(df, year).items():
                for cluster, capacity in cluster_dict.items():
                    records.append((area, cluster[1], cluster[0], year, capacity))

        df = pd.DataFrame(
            records,
            columns=[
                OutputMiscPowerColumns.AREA,
                OutputMiscPowerColumns.GROUP,
                OutputMiscPowerColumns.CLUSTER,
                "year",
                "capacity",
            ],
        )

        df_final = df.pivot_table(
            index=[OutputMiscPowerColumns.AREA, OutputMiscPowerColumns.GROUP, OutputMiscPowerColumns.CLUSTER],
            columns="year",
            values="capacity",
            fill_value=0,
        ).reset_index()

        df_final.columns.name = None

        # Add static values
        df_final[OutputMiscPowerColumns.TO_USE] = 1
        df_final[OutputMiscPowerColumns.CATEGORY] = MISC_CATEGORY_NAME

        # re-ordering columns
        first_cols = list(OutputMiscPowerColumns)
        df_final = df_final[first_cols + [c for c in df_final.columns if c not in first_cols]]

        return df_final

    def _export_misc_installed_power(self, df: pd.DataFrame) -> None:
        parent_dir = self.output_folder / MISC_INSTALL_POWER_FOLDER
        parent_dir.mkdir(parents=True, exist_ok=True)

        output_path = parent_dir / MISC_INSTALL_POWER_NAME_FILE
        df.to_excel(output_path, index=False)

    def build_misc_installed_power(self, df: pd.DataFrame) -> None:
        df = self._build_pegase_dataframe(df)
        self._export_misc_installed_power(df)
