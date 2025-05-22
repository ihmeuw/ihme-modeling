## NZL_NMDS data formatting 

The structure of `FILEPATH` has been updated and this code `01_format_NZL_NMDS_post_2017.do` exclusively formats new post-2017 NZL_NMDS data. The pre-2018 data processing script `01_format_NZL_NMDS_pre_2018.do` can still be found here. A new Python script (`consolidate_NZL_NMDS_data.py`) that appends all NZL_NMDS data is now in the directory as well. Additionally, the location of the plotting scripts has been updated as well – they are now in `FILEPATH` instead. The scripts there are updated to correctly plot for Maori and non-Maori populations. 


Differences between the two versions of Stata scripts:
1. We no longer deal with the live birth icd codes in this script, which was already the case for the 2016 - 2017 NZL_NMDS data.
2. `age_end` values are readjusted.
3. Data types for certain columns have changed.
4. We now only process and output new years' data in the Stata script and use `consolidate_NZL_NMDS_data.py` to check and append all data for better efficiency.


For future runs, please make sure that all outputs' names and new target years are updated properly in `01_format_NZL_NMDS_post_2017.do` and `consolidate_NZL_NMDS_data.py` first. Then proceed to run `01_format_NZL_NMDS_post_2017.do` and then `consolidate_NZL_NMDS_data.py`. Once the outputs are saved successfully, go to `FILEPATH`, run `mapping_icg_bundles.py`, followed by `ggplot_bundle_cf.R`.