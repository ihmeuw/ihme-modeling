NTD Ebola Execution

--------------
3 distinct data sources for each deaths/survivors
- imported
- endemic
- WA 2016 (historically kept separate)

all data is upscaled except for the imported cases
all-age data is split into proportions derived from the case data

---------------

~/child_scripts/utils.R
- contains inherited methods functions used in scripts

step_0_validate_parameters.R
- takes in data, creates output folders, and outputs underreporting, acute duration, and chronic duration (yr1 and yr2) draws in the draws directory

step_1_estimate_deaths.R
- writes out zeros for where no death data
- proportions out all-age deaths
- writes out imported deaths (no upscaling)
- writes out endemic deaths after upscaling
- saves all deaths file to account for in nonfatal meid ADDRESS1 ( dead encounter disability same year preceding death)

step_2_estimate_survivors.R
- writes out zeros where no deaths nor survivors
- proportions out all-age survivors
- upscales endemic survivors
- writes out meid ADDRESS1 including death estimates
- computes ADDRESS2 and its two year disability and writes out

* death is not uploaded to CodViz but flagged to CoDViz and updated here: URL-ADDRESS1 as well as shared in the ebola_handoffs slack channel

save_epi.R
- saves ADDRESS1 and ADDRESS2 sequentially 
- all years must be saved for these meids to prevent central interpolation

Technical updates to implement
-----------------
- Combining datasets together for ease and have flag for imported / endemic (WA classification not important)
- Seeing if more updated sex/age information availble post outbreaks
- Consistently monitoring additional ebola outbreaks and marking when data is last updated
- Refactoring complex sections of data management