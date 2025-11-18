# antares-data-collection

[![License](https://img.shields.io/github/license/AntaresSimulatorTeam/antares_craft)](https://mozilla.org/MPL/2.0/)


# NOTES 

# Several steps to use this package :

* Install `uv` and follow steps from doc ([Doc](https://docs.astral.sh/uv/))
* Clone repository `git clone https://github.com/AntaresSimulatorTeam/antares_data_collection.git`
* Put in the root directory and install dependencies (prod+dev)
  * `uv sync` 

## Linting and formatting

To reformat your code, use this command line: `uv ruff check src/ tests/ --fix && ruff format src/ tests/`

## Typechecking

To typecheck your code, use this command line: `uv run mypy src/`