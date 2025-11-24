# antares-data-collection

[![License](https://img.shields.io/github/license/AntaresSimulatorTeam/antares_craft)](https://mozilla.org/MPL/2.0/)


# NOTES 

# Several steps to use this package :

* Install `uv` and follow steps from doc ([Doc](https://docs.astral.sh/uv/))
* Install a specific version of Python with ` uv python install 3.11`
* Install `tox` with `uv tool install tox --with tox-uv`
* Clone repository `git clone https://github.com/AntaresSimulatorTeam/antares_data_collection.git`
* Put in the root directory and install dependencies (prod+dev)
  * `uv sync --locked --all-extras --dev` 

## Install Dependencies
Install all dependencies (prod + dev):  
`uv sync`

Install strictly from the lockfile:  
`uv sync --locked`

Install without dev dependencies:  
`uv sync --no-dev`

Install everything (all extras + dev):  
`uv sync --all-extras --dev`

## Add a Dependency
Add a production dependency:  
`uv add <package>`

Add a development dependency:  
`uv add --dev <package>`

Add a specific version:  
`uv add <package>==<version>`

## Remove a Dependency
`uv remove <package>`

## Update Dependencies
Regenerate the lockfile:  
`uv lock`

# Check/test package with `tox`
The configuration is specified in file `tox.ini`. But you can also call specific part of tests and
also for a specific Python version.

To simply launch all your tests :
  - use `tox` in bach

## Specific tests
Use `ruff` to check and format your code :
  - `tox -e lint`

Use `mypy` to check Typechecking : 
  - `tox -e type`

use `coverage` to check code coverage : 
  - `tox -e coverage`

By default `pytest` is run here wit `tox`. 
You just have to provide a version of tox environment like :
  - `tox  -e py3.11`

## Specific tests with specific env
You can mix command to run specific tests with specific `env` like :
* `tox  -e lint,type,py3.11`

or like : 
* `tox  -e lint,type,coverage,py3.11,py3.12`

# Possible to check/test without `tox`

## Linting and formatting
To reformat your code, use this command line: `uv ruff check src/ tests/ --fix && ruff format src/ tests/`

## Typechecking
To typecheck your code, use this command line: `uv run mypy src/`

