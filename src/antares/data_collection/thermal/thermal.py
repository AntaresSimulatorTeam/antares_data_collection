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
import pandas as pd

from antares.data_collection import LocalConfiguration
from antares.data_collection.referential_data.main_params import (
    THERMAL_TYPE_NAME,
    ClusterColumnsNames,
    CountryColumnsNames,
    MainParams,
)
from antares.data_collection.thermal.conf_thermal import (
    ThermalComputedColumns,
    ThermalDataColumns,
    ThermalDatetimeColumns,
    ThermalLayout,
)
from antares.data_collection.tools.tools import (
    scenario_filter,
    thermal_filter_active_years_commissioning,
)

# TODO steps of thermal process


# manage only one file "Thermal.csv"
def thermal_import(conf_input: LocalConfiguration) -> pd.DataFrame:
    # check files required
    conf_thermal_file = ThermalLayout()
    file_name = conf_thermal_file.input_data_name
    path_file = conf_input.input_path / file_name

    if not path_file.exists():
        raise ValueError(f"Input file does not exist: {path_file}")

    # read a file with only columns used + datetime columns
    list_col_to_use = [col.value for col in ThermalDataColumns]
    df = pd.read_csv(
        filepath_or_buffer=path_file,
        usecols=list_col_to_use,
        parse_dates=[
            ThermalDatetimeColumns.COMMISSIONING_DATE.value,
            ThermalDatetimeColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        ],
    )

    if df.empty:
        raise ValueError(f"Input file is empty: {path_file}")

    return df


# add code_antares from ref
# filter code antares with NA
# additional filter: columns "op_stat"
# add new columns: "BIO_MAX_GENERATION_MW" / "FOSSIL_MAX_GENERATION_MW"
def thermal_pre_treatments(
    df_thermal: pd.DataFrame,
    df_ref_pays: pd.DataFrame,
    df_ref_cluster: pd.DataFrame,
    op_stat: list[str] = ThermalLayout().default_values_column_op_stat,
) -> pd.DataFrame:

    # filter NA and keep only thermal cluster
    df_ref_cluster_filtered = df_ref_cluster[df_ref_cluster[ClusterColumnsNames.TYPE.value].eq(THERMAL_TYPE_NAME)]

    # merge with referential country
    df_thermal_updated = (
        pd.merge(
            df_thermal,
            df_ref_pays,
            left_on=ThermalDataColumns.MARKET_NODE.value,
            right_on=CountryColumnsNames.MARKET_NODE.value,
            how="left",
            validate="many_to_one",
        )
        .drop(
            columns=[
                CountryColumnsNames.NOM_PAYS.value,
                CountryColumnsNames.CODE_PAYS.value,
                CountryColumnsNames.AREAS.value,
                CountryColumnsNames.MARKET_NODE.value,
            ]
        )
        .dropna(subset=[CountryColumnsNames.CODE_ANTARES.value])
    )

    # merge with the referential cluster to bring CLUSTER_BP
    df_thermal_updated = pd.merge(
        df_thermal_updated,
        df_ref_cluster_filtered,
        left_on=ThermalDataColumns.PEMMDB_TECHNOLOGY.value,
        right_on=ClusterColumnsNames.CLUSTER_PEMMDB.value,
        how="left",
        validate="many_to_one",
    ).drop(
        columns=[
            ClusterColumnsNames.TYPE.value,
            ClusterColumnsNames.CLUSTER_PEMMDB.value,
        ]
    )

    # filter on op_stat
    df_thermal_updated = df_thermal_updated[df_thermal_updated[ThermalDataColumns.OP_STAT.value].isin(op_stat)]

    # add new columns FOSSIL_MAX_GENERATION_MW / BIO_MAX_GENERATION_MW
    df_thermal_updated = df_thermal_updated.assign(
        **{
            ThermalComputedColumns.BIO_MAX_GENERATION_MW.value: lambda d: np.where(
                d[ThermalDataColumns.SCND_FUEL.value].eq("Bio"),
                d[ThermalDataColumns.SCND_FUEL_RT.value] * d[ThermalDataColumns.NET_MAX_GEN_CAP.value],
                0,
            ),
            ThermalComputedColumns.FOSSIL_MAX_GENERATION_MW.value: lambda d: (
                d[ThermalDataColumns.NET_MAX_GEN_CAP.value] - d[ThermalComputedColumns.BIO_MAX_GENERATION_MW.value]
            ),
        }
    )

    # split to keep df BIO and df FOSSIL to keep only capacity on "NET_MAX_GEN_CAP"
    mask_bio = df_thermal_updated[ThermalComputedColumns.BIO_MAX_GENERATION_MW.value] > 0

    # tag bio to add a new cluster
    df_thermal_bio = (
        df_thermal_updated.loc[mask_bio]
        .drop(
            columns=[
                ThermalDataColumns.NET_MAX_GEN_CAP.value,
                ThermalComputedColumns.FOSSIL_MAX_GENERATION_MW.value,
            ]
        )
        .rename(columns={ThermalComputedColumns.BIO_MAX_GENERATION_MW.value: ThermalDataColumns.NET_MAX_GEN_CAP.value})
    )
    df_thermal_bio[ClusterColumnsNames.CLUSTER_BP.value] = df_thermal_bio[ClusterColumnsNames.CLUSTER_BP.value] + " bio"

    # manage FOSSIL
    mask_fossil = df_thermal_updated[ThermalComputedColumns.FOSSIL_MAX_GENERATION_MW.value] > 0
    df_thermal_fossil = (
        df_thermal_updated.loc[mask_fossil]
        .drop(
            columns=[
                ThermalDataColumns.NET_MAX_GEN_CAP.value,
                ThermalComputedColumns.BIO_MAX_GENERATION_MW.value,
            ]
        )
        .rename(
            columns={ThermalComputedColumns.FOSSIL_MAX_GENERATION_MW.value: ThermalDataColumns.NET_MAX_GEN_CAP.value}
        )
    )

    # concat
    frames = [df_thermal_bio, df_thermal_fossil]
    df_thermal_pre_treated = pd.concat(frames).reset_index(drop=True)

    # keep only useful columns
    list_cols_to_keep = [
        CountryColumnsNames.CODE_ANTARES.value,
        ThermalDataColumns.STUDY_SCENARIO.value,
        ThermalDataColumns.COMMISSIONING_DATE.value,
        ThermalDataColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        ClusterColumnsNames.CLUSTER_BP.value,
        ThermalDataColumns.NET_MAX_GEN_CAP.value,
    ]

    return df_thermal_pre_treated[list_cols_to_keep]


# TODO next steps
def thermal_treatments_year(
    df_thermal_pre_treated: pd.DataFrame,
    year_input: pd.Timestamp,
    filter_scenario_input: str,
) -> pd.DataFrame:
    # filter by year/month (Cutting years into months with overlapping format)
    # filter scenario by year
    # aggregate by code_antares/cluster_bp (power + number / unit count max to 100)
    # add new col date to tag year (int)

    # filter on date
    df_filtered = thermal_filter_active_years_commissioning(
        df_input=df_thermal_pre_treated,
        name_col_start_date=ThermalDatetimeColumns.COMMISSIONING_DATE.value,
        name_col_end_date=ThermalDatetimeColumns.DECOMMISSIONING_DATE_EXPECTED.value,
        year_date=year_input,
    )

    # filter on scenario
    df_filtered = scenario_filter(df_input=df_filtered, filter_params=filter_scenario_input)

    # compute sum aggregate by code_antares/cluster_bp (power + number / unit count max to 100)
    df_computed = thermal_compute_power_number_capacity(
        df_input=df_filtered,
        name_cols_index=[
            CountryColumnsNames.CODE_ANTARES.value,
            ClusterColumnsNames.CLUSTER_BP.value,
        ],
        name_capacity_col=ThermalDataColumns.NET_MAX_GEN_CAP.value,
    )

    return df_computed


def thermal_compute_power_number_capacity(
    df_input: pd.DataFrame, name_cols_index: list[str], name_capacity_col: str
) -> pd.DataFrame:

    df_aggregate = df_input.groupby(name_cols_index, as_index=False).agg(
        power=(name_capacity_col, "sum"), number=(name_capacity_col, "count")
    )

    df_aggregate["number"] = df_aggregate["number"].clip(upper=100)

    return df_aggregate


def thermal_manage_output_format() -> None:
    raise NotImplementedError("Not implemented yet")


def thermal_export() -> None:
    raise NotImplementedError("Not implemented yet")


# user function
def create_thermal_outputs(links_conf_input: LocalConfiguration, referential_data: MainParams) -> None:
    # call thermal_import(links_conf_input)
    #    use parse_main_params()

    # call thermal_pre_treatments(df_thermal: pd.DataFrame,
    #     df_ref_pays: pd.DataFrame,
    #     df_ref_cluster: pd.DataFrame,
    #     op_stat: list[str] = ThermalLayout().default_values_column_op_stat)

    # call thermal_treatments_year()

    # cal thermal_export()
    raise NotImplementedError("Not implemented yet")
