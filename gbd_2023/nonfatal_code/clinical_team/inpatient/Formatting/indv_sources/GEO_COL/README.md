# GEO_COL data formatting
Last update March 2023

For previous version of the GEO formatting, see the archive folder in this directory.

This GBD formatting code was adapted from the GEO_COL utilization envelope formatting script at `FILEPATH` to operate in the manner of the ITA_HID formatting code. The `format_GEO_COL.py` is designed to run on all years of data at once with the `geo_worker.py` script, concatenating them with the `concat_GEO_COL_worker.py` script and saving to `/FILEPATH`. 

At present, the concatenation script's validation check fails when comparing against the archived script's latest output, but the sum of the current script's "val" column was manually validated against the utilization envelope script output's total number of rows instead. The concat script's validation function is intended to be used for future data's comparison against the current output.