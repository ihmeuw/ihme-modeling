# Probability of Death

## Summary

Calculates probability of death for specified demographics, GBD round, and decomp step.
Probability of death is calculated for certain age group aggregates using the following method:

1. Subset to age groups that comprise the aggregate
2. Save the survivorship for the youngest age group
3. Compute age-specific deaths as survivorship * probability of death * cause fraction
4. Aggregate (3) by age then divide by (2)

Survivorship (lx) and probability of death (qx) are pulled from life table summaries, and deaths cause fraction is pulled from `get_outputs`.

## Installation

Prerequisites: conda installation, NO poetry installation (in order to work with conda and jobmon, poetry must be installed in a conda environment rather than globally).

Create and activate conda environment for this package:

    $ conda create -n probability_of_death python=3.7
    $ conda activate probability_of_death

Install packages that must be installed through conda:

    $ conda install -c conda-forge mysql-connector-c mysqlclient

Clone the repo, install poetry, install the package, and build:

    $ git clone ADDRESS
    $ cd [path_to_this_repo]
    $ pip install poetry
    $ poetry update
    $ poetry install
    $ nox

To view the nox targets:

    $ nox -l

## Usage

Run the workflow via `poetry run workflow` with the following arguments:

- [`decomp_step`] Required.
- [`gbd_round_id`] Required.
- [`--location_set_ids`] Optional. Comma-separated list of IDs. Location sets whose locations will be used in probability of death calculation. Defaults to "3,5,11,20,24,26,28,31,32,46,35,40"
- [`--year_ids`] Optional. Comma-separated list of IDs. Defaults to "1990, ..., <current_GBD_round>"
- [`--sex_ids`] Optional. Comma-separated list of IDs. Defaults to "1,2,3"
- [`--age_group_ids`] Optional. Comma-separated list of IDs. Defaults to "5, ...,20,28,30,31,32"
- [`--cluster_project`] Optional. Default proj_centralcomp.
- [`--deaths_version`] Optional. Whether to call `get_outputs` with "best" or "latest". Default "best".

The age group aggregates are specified in lib/generate.py. They may need to be updated each round.

## Areas for Improvement

- Add unit tests and integration tests
- Store metadata associated with a probability of death run
