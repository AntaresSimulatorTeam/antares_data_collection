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

from antares.data_collection.links.parsing.links import LinksParser
from antares.data_collection.referential_data.main_params import parse_main_params
from tests.conftest import INPUT_RESOURCE_PATH


def test_nominal_case(tmp_path: Path) -> None:
    # Use the real MainParams file
    main_params = parse_main_params(INPUT_RESOURCE_PATH / "MAIN_PARAMS_2025.xlsx")

    # Build a thermal installed power file
    parser = LinksParser(INPUT_RESOURCE_PATH / "links", tmp_path, main_params, [2030])
    parser._fill_ntc_index_map()
