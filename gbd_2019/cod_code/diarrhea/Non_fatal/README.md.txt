The code in this folder is grouped together as code that prepares the non-fatal data
for diarrhea modeling. The files must be run in the order listed below:

1* diarrhea_survey_processing.R: This code does a fair amount of work to process,
collapse, and tabulate the survey data for diarrhea. Much of these data are coming from
UbCov outputs (a tool that centralizes survey standardization and extraction). The code
collapses the survey data to survey-month results and uses those data in a mixed effects
regression to find a scalar for survey-month to adjust the survey data to be representative of the
entire year. It then collapses by age-sex-survey-year the survey-weighted mean two-week
period prevalence of diarrhea. 

1* diarrhea_survey_processing_single_survey.R: This code is essentially the same
function as the file above. The key difference is that above, all of the individual
survey results must be read into memory at once (HUGE!). This file is reconstructed
such that each survey is imported separately and tabulated. It does, however, require
that the seasonality scalars have been previously calculated.

2* diarrhea_convert_survey_period-point.R: This file converts the two-week period prevalence
determined in the file above to point prevalence (the modeled value in DisMod).

3* prep_survey_upload.R: This file does some cleaning and formatting required to upload the
survey data to the Epi database, including clearing the previous survey data.

4* crosswalk_diarrhea_all_mr-brt.R: This file performs the crosswalks and data pulling/processing
necessary for uploading and modeling. There are a series of crosswalks including inpatient
hospitalization from scientific literature, inpatient claims data, and Marketscan claims data.
A more detailed description of the modeling approach for crosswalks can be found in the diarrhea
documentation. Lastly, this file saves and uploads those data.

5* diarrhea_emr_central_function.R: This file pulls excess mortality rates (EMR) from a 
completed DisMod model (should be step 3) and runs an MR-BRT regression to predict
EMR for all locations, by sex and age, dependent on healthcare access and quality index (HAQI).
The code then preps and uploads the data either to:
	A) Existing bundle (bundle_id 3) for step 4
	B) New bundle for step 3
