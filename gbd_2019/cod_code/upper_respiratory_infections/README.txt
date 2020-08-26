########################################################################
## Upper respiratory infection non-fatal modeling is relatively
## straightforward, there is no clinical data or crosswalks
## that are used, so the code here is simply to collapse
## survey data (same survey extractions as LRI but a different
## case definition) and to upload the data to the bundle for modeling.
########################################################################

## Used ##
* uri_collapse_survey_data_all.R: This file is the file that takes the results from the UbCov
	survey cleaning (from the LRI codebook) and collapses to the GBD location by child
	sex and age in 1-year increments. It uses the survey weights from the surveys to
	adjust the mean prevalence. A place for improvement/added consistency with the LRI
	collapse would be to add an adjustment for seasonality. We would hypothesize that
	URI prevalence might be seasonal in many locations. Since the surveys only occur
	over a period of several months (typically), they are unlikely to be representative
	of the entire year. The LRI collapse code uses a periodic regression for each GBD
	region to determine a month-specific scalar so that some prevalence values are 
	increased and some are decreased to be representative of the entire year. 

* prep_uri_data_decomp2.R: This file just cleans and preps data to be uploaded to the 
	Epi database in the URI bundle.

