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

import pytest
from pathlib import Path
from antares_data_collection.import_files.import_config import ImportConfig


class TestImportConfig:
    def test_file_not_exist(self) -> None:
        # then
        with pytest.raises(ValueError, match="does not exist"):
            # when
            ImportConfig(path=Path("data_not_exist.csv"))

    def test_infer_file_type(self, tmp_path: Path) -> None:
        # create a fake csv file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3")

        cfg = ImportConfig(path=csv_file)

        assert cfg.file_type == "csv"
