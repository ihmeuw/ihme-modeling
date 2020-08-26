# Dismod Age/Sex Splits

## Purpose:
For data where we can reasonably trust the age and sex patterns of the best GBD2019 Decomposition Step 1 results. This code pulls model data to generate a pattern, and applies those splits to bundle data. The goal is to generate an input data sheet that can be fed into MR-BRT, to determine crosswalk ratios per covariate  definition.

#### do_dismod_split.R
This is the main script. This script calls on functions in **functions_dismod_split.R**. There are four patterns of splitting: (1) Sex-specific (M/F) & within a GBD age range, (2) Sex-specific (M/F) & wider than a GBD age range, (3) Sex-aggregated (Both) & within a GBD age range, (4) Sex-aggregated (Both) & wider than a GBD age range.

Patterns 2-4 must be adjusted to match pattern 1. This is done by applying a relevant age-sex weight to both the cases and sample size of an estimate. 

#### functions_dismod_split.R
These are the suite of functions called by **do_dismod_split.R**. Essentially, weights are derived from model data, and applied onto bundle data. Once data has been properly age/sex split, then the matching process links reference to alternate data (by age/sex/location/year).

#### bundle_functions.R
This file contains two functions. The first functions, bundle_subset, takes in a bundle id. The function reads the bundle into a data table, subsets the data on age (removing any entries >=70 years old), and also removes any "cv_marketscan" columns that are not specifically "cv_marketscan".
Note: This function is only to be used for hemoglobinopathy bundles (206-212). The second function, submit_bundle_crosswalk, submits a crosswalk job by bundle and measure.

### TO-DO LIST:
	- finish both sex splits
	- impute and adjust standard error using Wilson's formula
	- impute cases & sample size where missing
	- finalize matching process
	- calculate standard error of the ratio
	- account for congenital and neonatal specificities
