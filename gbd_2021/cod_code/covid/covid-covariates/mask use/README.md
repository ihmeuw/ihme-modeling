## Summary of mask use repository

This repository contains the code necessary to create estimates of the mask use covariate for the IHME SEIR model. 

**[HUB page of documentation](https://hub.ihme.washington.edu/display/CVC/Mask+use+covariate)**

The pipeline can be run using a single script

*mask_use.R*

Which sources several other functions to make smoothed daily estimates of mask use based on Facebook self-reported surveys,
YouGov surveys, PREMISE Surveys, and the Kaiser Family Foundation surveys. Projections into the future are
created for the reference scenario in *mask_use.R* and long-range scenarios with different assumptions about how
mask use might change with vaccine coverage, COVID-19 seasonality, are sourced in *make_long_range_scenarios.R*. 

Maps for mask use can be found in the *map_mask_use.R* script.
	
Outputs are saved here including all diagnostics *"/ihme/covid-19-2/mask_use/{model_version_id}"*.

The files that go into SEIR are the **mask_use.csv**, **mask_use_worse.csv** and **mask_use_best.csv** files. 