USERNAME
USERNAME
03/2019
Documentation for HAP ubcov extraction code

Step 0: Codebooking
	For GBD 2019, we are switching to the LBD hap codebook which will save us a lot of time, as they have already codebooked most of the surveys we'll be using. However, as of 2019, LBD doesn't use pre-2000 surveys, surveys from stage 3 countries, surveys from coutries outside of their modeling regions, etc (see hap codebook-->lbd notes), so we must diligently check all surveys to ensure we are accurately extracting each and every indicator used in the GBD. We must also be careful not to interfere with LBD's work by documenting changes we make.

	We have found that we mostly use the following five spreadsheets in ubcov: basic, label, merge, hap codebook, and hap map.

	Basic workflow:
	Check NID in the hap codebook
	-If NID is properly codebooked, mark as such in hap 2019 source list.
	-If NID in not properly codebooked, codebook using instructions below.
		-If you need to make a merge, check to make sure the master file is in basic before you enter it in merge.
		-If you need to label variables, do so in label.


	HAP codebook instructions for GBD 2019 (relevant indicators):
	-gbd assigned: name here if you make changes
	-gbd notes: notes were if you make changes, encounter issues, etc
	-exclude_reason: notes here if you believe the survey should be excluded
	-date: date here if you make changes
	-***the next three columns are for LBD tracking and will help you know if you need to check the .dta or you can mark as "codebooked"--check "lbd notes/color key" sheet for info
	-ubcov_id: should populate automatically if survey_name, nid, ihme_loc_id, year_start, -year_end, survey module, and file_path are filled out correctly (see next line)
	***the next seven columns should match the survey's entry in the basic codebook exactly
	-hh_size: must be present if survey_module=HH; sometimes this variable will only be present in another module. In that case, it much be merged onto the master file in merge
	-cooking_fuel: variable if present and mapped in the hap map.
	-heating_fuel: variable here if present in survey (check questionnaire and .dtas)
	-lighting_fuel: variable here if present in survey (check questionnaire and .dtas)
	-housing_roof: variable here if present in survey (check questionnaire and .dtas)
	-housing_wall: variable here if present in survey (check questionnaire and .dtas)
	-housing_floor: variable here if present in survey (check questionnaire and .dtas)
	***the next three columns are a repeat of the previous three (please duplicate the roof/wall/floor variables)--they are for an experiment jfrostad is conducting


Step 1: extract.do

	This is a stata script that extracts the surveys from the ubcov codebook and saves them on the ADDRESS drive. 
	The output is saved in FILEPATH or FILEPATH 
	To run, check the outpath, and enter the ubcov_ids after array. 
	The run_extract command will pull the ubcov_id, check to make sure everything is mapped correctly, and show you the output. 
	If you don't want to see the output file every time, you can change it to "run_extract i, bypass".
	If you have to remap something (ex: GBD subnat or indicator value), you WILL HAVE TO RERUN EXTRACTION on this survey. 

	There is also a script called "batch_extract.do" that uses the cluster to extract in parallel. 
	This is much faster for running a lot of extractions, but you do not have the benefits of interactive value mapping and data vizualization. 
	I think this may be helpful if we have to make any changes to code and re-extract a lot of data.
	The code has not been updated or tested, but we can cross that bridge when we get there!	


Step 2: custom_code.R (b_extract_prepped)

	This is an R script that can be run locally, but will run more quickly on the cluster.
	It pulls the extracted data from the "a_ubcov_extract" folder on the ADDRESS or ADDRESS drive, runs some validations, generates indicators of missingness, generates all the fuel-type indicators, and checks the dataset if we are able to collapse at both household and individual level.
	It saves this prepped dataset as a .csv in the "FILEPATH " folder labeled by the date on the FILEPATH or FILEPATH folder labeled by the date on the ADDRESS drive. 

	There are two toggles at the beginning of the code. 
	The limited_use toggle is to determine if we are working with limited use or data from the ADDRESS drive. 
	If prep_all is T, it will prep everything in the ADDDRESS or ADDRESS "FILEPATH" folder. Otherwise, it will read in a logfile to see what has already been prepped and just prep the ".csv"s that haven't been done yet. 
	If you know a file needs to be reprepped, you can open the log and change the reprep column to 1. FILEPATH
	After it finishes it updates the log with all of the source metadata and the date prepped. 

	I have written in warnings in the code to check to see if we are subnationally mapping as detailed as possible, if there are an abundance of very large households, and if we are extracting all the variables we need for hh weighting.
	In some cases, the extraction may be correct, even if the warning appears, it is just a reminder to check the .DTA and documentation for additional variables if possible and to make sure we are getting the most information we can out of each survey. 

	Methodological Decisions:
		If household size is missing or over 35 (seems unreasonably large), we are replacing with the median household size of a given survey. 
		When collapsing to the HH level (cv_HH=1), our first choice is to use the hhweight. When this is not available, we are using the mean pweight for the given household. I don't know why this would differ by household, but I took the mean just in case. If none are available we just weight each house equally, because that is what the collapse code does.
		For HH modules to collapse to the individual level (cv_HH=0), we must have household size. Our first choice is to set pweight = hhweight * hhsize. Thes second choice is to just use hhsize to weight (assuming survey is represenatative). If we do not have hhsize, we cannot collapse to individual level.
		We are dropping other, unknown, and missing fuel type and recording the percent missingness. This assumes that missingness is non-differential by fuel type.



Step 3: collapse.R

	This is an R script that must be run on a particular version of R. (This one on the cluster works: "FILEPATH ").
	The purpose is to take all of the microdata in the FILEPATH folders on the ADDRESS drive or "FILEPATH" on the ADDRESS drive and tabulate it using central code. 
	It reads in all files in a given folder, so I reccommend running this once per day on a folder such as "FILEPATH".
	To specify the folder of surveys you want to collapse, you have to open FILEPATH and change the input directory. 

	In addition to editing the config, there are also toggles in the code settings. 
	If parallel = T, it launches a separate cluster job for each collapse. 
	This is good if you are running a lot of surveys at the same time. 
	Otherwise, it will run them serially in one R session.
	Most collapses will be fairly fast, but some (ex: Censuses, IPUMS) may take a while and should be run with higher number of resources on the cluster (i.e. 20 slots).

	It outputs a .csv in the "FILEPATH" folder that is labeled by date. 
	In the output dataset, each survey should have at least 9 rows corresponding to the 8 fuel type categories and a row for the proportion using solid fuel. 
	For most surveys, if we have sufficient data on hhsize or unique identifiers for households, we should be able to collapse at the household and the individual level. 
	This is indicated by cv_HH. If cv_HH=1, that row represents the proportion of households. If cv_HH=0, that row represents the proportion of individuals. Typically we would expect the proportion of households using dirty fuels to be lower than the proportion of individuals because we would expect larger household size to be associated with dirtier fuels. 
	There will also be different rows for each ihme_loc_id available in the dataset. 



Step 4: save_bundle.R

	I haven't written this code yet, but in theory it will check to make sure the data is not already in the bundle, and upload/overwrite as appropriate. I may need different bundles for each fuel type. If so, this code will also split the files accordingly. 
