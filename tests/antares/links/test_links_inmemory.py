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
from typing import Any, Dict

import pandas as pd
import pytest
from pathlib import Path

from antares.data_collection.links import conf_links, links
from antares.data_collection.tools.conf import LocalConfiguration


@pytest.fixture()
def mock_inmemory_readers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """
    Provide in-memory CSV/Excel data for links.create_links_part without creating real files.

    It monkeypatches:
      - Path.exists to simulate the presence of the three CSV inputs and the Excel refs file
      - pandas.read_csv to return in-memory DataFrames based on filename
      - pandas.read_excel to return in-memory DataFrames based on sheet_name
    """
    cfg = conf_links.LinksFileConfig()

    # ----- Build minimal in-memory datasets -----
    # Reference sheets (read via read_excel)
    excel_sheets: Dict[str, pd.DataFrame] = {
        # Used to map market nodes to Antares codes for source/destination merges
        "LINKS": pd.DataFrame(
            {
                "market_node": ["AL00", "AT00"],
                "code_antares": ["AL", "AT"],
            }
        ),
        # Used to map hour/month to periods for grouping
        "PEAK_PARAMS": pd.DataFrame(
            {
                "hour": [1, 2],
                "period_hour": ["HC", "HP"],
                "month": [1, 2],
                "period_month": ["winter", "summer"],
            }
        ),
        # Used to pick study scenario by calendar year (value itself is ignored by current impl)
        "STUDY_SCENARIO": pd.DataFrame(
            {
                "YEAR": [2030, 2060],
                "STUDY_SCENARIO": ["ERAA", "TYNDP"],
            }
        ),
    }

    # Timeseries NTCs: minimal structure for grouping/median
    # Columns: HOUR, MONTH, DAY and at least one numeric curve column
    ntcs_df = pd.DataFrame(
        {
            "HOUR": [1, 2],
            "MONTH": [1, 2],
            "DAY": [1, 1],
            # Curve UID as column name with numeric values
            "curve1": [100.0, 200.0],
        }
    )

    # NTCs Index: must include LABEL, COUNT to be dropped, and ID/CURVE_UID/ZONE
    ntc_index_df = pd.DataFrame(
        {
            "LABEL": ["dummy"],
            "COUNT": [1],
            "ID": ["C1"],
            "CURVE_UID": ["curve1"],
            "ZONE": ["Z1"],
        }
    )

    # Transfer Links: will be filtered to NTC + HVAC and merged with NTC index
    transfer_links_df = pd.DataFrame(
        {
            "ZONE": ["Z1"],
            "NTC_CURVE_ID": ["C1"],
            "TRANSFER_TYPE": ["NTC"],
            "TRANSFER_TECHNOLOGY": ["HVAC"],
            "MARKET_ZONE_SOURCE": ["AL00"],
            "MARKET_ZONE_DESTINATION": ["AT00"],
            "STUDY_SCENARIO": ["ERAA"],
            "YEAR_VALID_START": [2020],
            "YEAR_VALID_END": [2100],
        }
    )

    csv_by_name: Dict[str, pd.DataFrame] = {
        cfg.NTC_TS: ntcs_df,
        cfg.NTC_INDEX: ntc_index_df,
        cfg.TRANSFER_LINKS: transfer_links_df,
    }

    # Prepare a dummy excel path which will be reported as existing
    dummy_excel_path = tmp_path / "refs.xlsx"

    # Save originals to delegate for other paths
    original_exists = Path.exists

    def fake_exists(self: Path) -> bool:  # type: ignore[override]
        # Simulate existence for the expected input CSV files under input_path
        # and for the dummy excel file path.
        if self == dummy_excel_path:
            return True
        # For CSVs, we only check the name matches the expected config names
        if self.name in csv_by_name:
            return True
        # Fall back to real behavior (e.g., tmp_path directories do exist)
        return original_exists(self)

    def fake_read_csv(filepath: Any, *args: Any, **kwargs: Any) -> pd.DataFrame:
        name = os.fspath(filepath)
        base = os.path.basename(name)
        if base in csv_by_name:
            return csv_by_name[base].copy()
        raise AssertionError(f"Unexpected CSV requested: {filepath}")

    def fake_read_excel(filepath: Any, *args: Any, **kwargs: Any) -> pd.DataFrame:
        # filepath should be our dummy_excel_path; we ignore it and choose by sheet_name
        sheet = kwargs.get("sheet_name")
        if sheet in excel_sheets:
            return excel_sheets[sheet].copy()
        raise AssertionError(f"Unexpected Excel sheet requested: {sheet}")

    # Apply monkeypatches
    monkeypatch.setattr(Path, "exists", fake_exists, raising=False)
    monkeypatch.setattr(pd, "read_csv", fake_read_csv, raising=True)
    monkeypatch.setattr(pd, "read_excel", fake_read_excel, raising=True)

    return dummy_excel_path


def test_create_links_part_inmemory(mock_inmemory_readers: Path, tmp_path: Path) -> None:
    """End-to-end smoke test of create_links_part using only in-memory data.

    Verifies that the function runs to completion without touching real CSV/XLSX files.
    """
    local_conf = LocalConfiguration(
        input_path=tmp_path,  # directory exists (pytest tmp_path)
        export_path=tmp_path,
        scenario_name="test",
        data_references_path=mock_inmemory_readers,  # a path we report as existing
        calendar_year=[2030, 2060],
    )

    # Should not raise
    links.create_links_part(conf_input=local_conf)
