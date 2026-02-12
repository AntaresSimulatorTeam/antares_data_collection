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
from antares.data_collection.referential_data.struct_main_params import (
    CountryColumnsNames, ClusterColumnsNames,
)
from antares.data_collection.thermal.conf_thermal import (
    ThermalLayout,
    ThermalDataColumns,
    ThermalComputedColumns,
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

    # read a file with only columns used
    list_col_to_use = [col.value for col in ThermalDataColumns]
    df = pd.read_csv(filepath_or_buffer=path_file, usecols=list_col_to_use)

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
    df_thermal_updated = (
        pd.merge(
            df_thermal_updated,
            df_ref_cluster,
            left_on=ThermalDataColumns.PEMMDB_TECHNOLOGY.value,
            right_on=ClusterColumnsNames.CLUSTER_PEMMDB.value,
            how="left",
            validate="many_to_one")
        .drop(columns=[ClusterColumnsNames.TYPE.value])
    )

    # filter on op_stat
    df_thermal_updated = df_thermal_updated[
        df_thermal_updated[ThermalDataColumns.OP_STAT.value].isin(op_stat)
    ]

    # add new columns FOSSIL_MAX_GENERATION_MW / BIO_MAX_GENERATION_MW
    df_thermal_updated = df_thermal_updated.assign(
        **{
            ThermalComputedColumns.BIO_MAX_GENERATION_MW.value: lambda d: np.where(
                d["SCND_FUEL"].eq("Bio"), d["SCND_FUEL_RT"] * d["NET_MAX_GEN_CAP"], 0
            ),
            ThermalComputedColumns.FOSSIL_MAX_GENERATION_MW.value: lambda d: (
                d["NET_MAX_GEN_CAP"]
                - d[ThermalComputedColumns.BIO_MAX_GENERATION_MW.value]
            ),
        }
    )

    # split to keep df BIO and df FOSSIL to keep only capacity on "NET_MAX_GEN_CAP"
    mask_bio = df_thermal_updated["BIO_MAX_GENERATION_MW"] > 0

    # tag bio to add a new cluster
    df_thermal_bio = (
        df_thermal_updated.loc[mask_bio]
        .drop(columns=["NET_MAX_GEN_CAP", "FOSSIL_MAX_GENERATION_MW"])
        .rename(columns={"BIO_MAX_GENERATION_MW": "NET_MAX_GEN_CAP"})
    )
    df_thermal_bio[ClusterColumnsNames.CLUSTER_BP.value] = df_thermal_bio[ClusterColumnsNames.CLUSTER_BP.value] + " bio"

    # manage FOSSIL
    mask_fossil = df_thermal_updated["FOSSIL_MAX_GENERATION_MW"] > 0
    df_thermal_fossil = (
        df_thermal_updated.loc[mask_fossil]
        .drop(columns=["NET_MAX_GEN_CAP", "BIO_MAX_GENERATION_MW"])
        .rename(columns={"FOSSIL_MAX_GENERATION_MW": "NET_MAX_GEN_CAP"})
    )

    # concat
    frames = [df_thermal_bio, df_thermal_fossil]
    df_thermal_pre_treated = pd.concat(frames).reset_index(drop=True)

    # keep only useful columns
    return df_thermal_pre_treated





# TODO next steps
def thermal_treatments_year() -> None:
    raise NotImplementedError("Not implemented yet")


def thermal_export() -> None:
    raise NotImplementedError("Not implemented yet")
