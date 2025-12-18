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


class LinksFileConfig:
    def __init__(self) -> None:
        self.NTC_INDEX = "NTCs Index.csv"
        self.NTC_TS = "NTCs.csv"
        self.TRANSFER_LINKS = "Transfer Links.csv"

    def all_names(self) -> list[str]:
        return [self.NTC_INDEX, self.NTC_TS, self.TRANSFER_LINKS]
