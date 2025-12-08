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
from typing import Any

from antares.data_collection.links import conf_links


def create_links_part(dir_input: Path, dir_output: Path, **kwargs: Any) -> None:
    # check input/output directory
    if not dir_input.is_dir():
        raise ValueError(f"Input directory {dir_input} does not exist.")

    if not dir_output.is_dir():
        raise ValueError(f"Output directory {dir_output} does not exist.")

    for file_name in conf_links.LinksFileNames().files:
        path_file = dir_input / file_name
        if not path_file.exists():
            raise ValueError(f"Input file does not exist: {path_file}")
