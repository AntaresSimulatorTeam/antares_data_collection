# antares-data-collection

[![License](https://img.shields.io/github/license/AntaresSimulatorTeam/antares_craft)](https://mozilla.org/MPL/2.0/)
[![Python CI with Coverage and Sonar](https://github.com/AntaresSimulatorTeam/antares_data_collection/actions/workflows/coverage.yml/badge.svg)](https://github.com/AntaresSimulatorTeam/antares_data_collection/actions/workflows/coverage.yml)

## Installation

antares-data-collection can simply be installed from PyPI repository, typically using pip:

```shell
pip install antares-data-collection
```

## Example of use

from antares.data_collection import PEMMDBConverter

years = [2030, 2035]
converter = PEMMDBConverter(input_folder, output_folder, main_params_path, years)

### Thermals
op_stat = ["Available on market", "Inelastic supply / fixed profile"]  
converter.build_thermal_files(op_stat)
### Dsr
converter.build_dsr_files(op_stat, ["Demand shedding", "Demand shifting"], [-1])
### Batteries
converter.build_batteries_files()
### Links
converter.build_link_files()
### Misc
converter.build_misc_files(op_stat)
