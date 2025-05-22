# NLD_NIVEL data formatting
Last update June 2024

This Netherlands General Practitioner formatting script was developed in stages according to some limitations and inconsistencies in the raw data we received.

The data received arrived in two versions: a first, complete data set with causes in both the broader (5-14, 15-49 etc.) age bins and narrower (5-9, 10-14) age bins; and a second, incomplete data set which had some causes' data moved from the broader to narrower age bins, but which had a variety of data points missing which we had to import from the first dataset.

After merging the two datasets, a number of calculations and adjustments are made to the data by these scripts. First, we calculated the incidence and prevalence cases from the rates in the raw data by multiplying the rates in each cell by their corresponding year/age_bin's population value, and dividing the result by 1,000. Second, we adjusted the resulting prevalence cases by subtracting half of the incidence cases from them.

In order to adjust the prevalence cases in step two above, a number of operations needed to be made upon the data. First, since not all of the incidence/prevalence pairs had their values within the same sets of age bins, we aggregated any granular prevalence values without matching granular incidence values into their broader age bins with matching broad incidence values. After performing this calculation, we then disaggregated any previously aggregated prevalences back into their original granular age bins.

In addition to this, for the Dementia cause in particular we generated new estimates for its 50-54 and 55-59 age bins, based upon the difference between its 60-64/65-69 and 50-69 age bins in the second and first datasets, respectively.

To summarizes these processes across the script files in the NLD pipeline:

#### 01_NLD_NIVEL_combine-datasets.py: 
Merges version 1 and version 2 of the data, adding broader age bins from version 1 where bins are missing in version 2 while keeping the added granularity of version 2 where it exists.

#### 02_NLD_NIVEL_intermediate-aggregate.py: 
Performs the primary formatting and rate-to-case calculations on the merged raw datafile, and aggregates mismatched granular prevalence values with their matching broader-age-bin incidence values. Creates two outputs: an "...intermediate.xlsx" file used in the "03_NLD_NIVEL_adjusted-aggregate.py" script, and an "...intermediate-harmonized.xlsx" file used in the "06_NLD_NIVEL_disaggregate.py" script with the unadjusted granular Dementia estimates.

#### 03_NLD_NIVEL_adjusted-aggregate.py: 
Performs the incidence/prevalence adjustment on the broad-age-bin aggregated prevalence values. After performing the prevalence adjustments on a temporary 50-59 age bin for Dementia, this script also then generates the adjusted granular Dementia estimates separately. 

#### 04_NLD_NIVEL_unadjusted.py: 
This script also performs the primary formatting and rate-to-case calculations like "02_NLD_NIVEL_intermediate-aggregate.py", but does not aggregate the mismatching prevalence/incidence pairs. Its output is eventually used in the "06_NLD_NIVEL_disaggregate.py" script to disaggregate the aggregated prevalence values from scripts "02/03_NLD_NIVEL..." above.

#### 05_NLD_NIVEL_pivot-long.R:
Pivots the three "unadjusted.xlsx", "intermediate-harmonized.xlsx" and "adjusted-aggregate.xlsx" files from wide to long format for "06_NLD_NIVEL_disaggregate.py".

#### 06_NLD_NIVEL_disaggregate.py:
Uses the three long-format outputs from "05_NLD_NIVEL_pivot-long.R" to identify which rows had been aggregated for the "intermediate"/"adjusted" files within the "unadjusted" file, adjusts those rows within the "unadjusted" file based upon the ratio between the "intermediate" and "adjusted" values and the relative population proportions of the unadjusted values, and merges on the newly adjusted granular rows to replace their equivalent broader rows in the long-format "adjusted-aggregate.csv" file to create the final output.