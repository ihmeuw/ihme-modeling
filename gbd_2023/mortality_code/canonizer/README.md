# Canonizer

> Create a canonical set of mortality results from OneMod mortality estimates


## Overview

The goal of this process is to create a "canonical" set of life tables that
can then be paired with population estimates to create a consistent all-cause
mortality envelope. Broadly, the steps are:

1. Calculate ax and qx from input single year mortality rates
2. Add shock-specific mortality
3. Create aggregate locations (potentially including special location sets)
4. Calculate abridged life tables and envelope
5. Calculate summary outputs
6. Format outputs for consumption by other teams

### Inputs

This process directly requires processed OneMod mortality
rate estimates in most detailed canonical age groups and population
estimates (both abridged and single year) associated with the same mortality
pipeline.

Additionally, this process requires shock deaths estimated by other teams.


### Outputs

The outputs of this process are two pairs of life tables and envelopes:

2. With HIV, no shocks
3. With HIV, with shocks
