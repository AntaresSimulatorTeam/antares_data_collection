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
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator


class ImportConfig(BaseModel):
    """
    Configuration object describing how a file should be imported.

    This class:
    - Validates that the provided path exists.
    - Infers the file type (csv, excel, parquet) based on the file extension if not provided.
    - Stores additional import parameters such as separator, sheet name, and encoding.

    Attributes:
        path (Path): Path to the input file. Must exist on the filesystem.
        sep (str | None): Field delimiter for CSV/TXT files. Defaults to ",".
        sheet_name (str | None): Sheet name for Excel files.
        encoding (str | None): Optional file encoding.
        file_type (str | None): File type ("csv", "excel", "parquet", ...).
            If None, the type is inferred from the file extension.
    """

    path: Path
    sep: Optional[str] = ","
    sheet_name: Optional[str] = None
    encoding: Optional[str] = None
    file_type: Optional[str] = None

    @field_validator("path")
    def check_file_exists(cls, v: Path) -> Path:
        if not v.is_file():
            raise ValueError(f"The file {v} does not exist.")
        return v

    @model_validator(mode="after")
    def set_file_type(self) -> "ImportConfig":
        if self.file_type is None:
            suffix = self.path.suffix.lower()
            if suffix in {".csv", ".txt", ".tsv"}:
                self.file_type = "csv"
            elif suffix in {".xls", ".xlsx"}:
                self.file_type = "excel"
            elif suffix == ".parquet":
                self.file_type = "parquet"
        return self
