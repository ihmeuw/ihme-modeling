# Canonizer

> Create canonical set of mortality results from model life table and under-5
envelope estimates


## Overview

The goal of this process is to create a "canonical" set of life tables that
can then be paired with population estimates to create a consistent all-cause
mortality envelope. Broadly, the steps are:

1. Convert abridged model life table estimates to single year ages
2. Reckon HIV
3. Add shocks
4. Create aggregate locations (potentially including special location sets)
5. Calculate abridged life tables and envelope
6. Calculate summary outputs
7. Format outputs for consumption by other teams

### Inputs

This process requires model life table estimate and
under-5 envelope estimates from earlier steps in the mortality
pipeline, and population estimates (both abridged and single year)
associated with the same mortality pipeline.

Additionally, this process requires shock deaths and and HIV estimates provided
by other teams.


### Outputs

The outputs of this process are three pairs of life tables and envelopes:

1. No HIV, no shocks
2. With HIV, no shocks
3. With HIV, with shocks
