# Producing heart failure etiology proportion from Marketscan and CoDcorrect estimates as inputs for DisMod 
+ __FILES__:
	1. _00_prep_hf_mktscan_parallel.py_ -- this is the main script; it calls all the other functions and processes of the program.
	2. _hf_processes.py_ -- Has 3 classes each runs a different process. 
		* _Marketscan_ gets the marketscan data and prepares it to be merged
	with the CoDcorrect after it is pulled. Since the Marketscan doesn't inherently have uncertainty, a beta distribution is used to estimate
	the standard error (the standard devation of the mean) and then calculate the coefficient of variation. Also, in addition to the 21 standard
	HF causes, 3 custom composite causes are made as aggregates of other causes. There is an age restriction as follows:
	certain causes have will have age cutoffs of 30, where all proportions below 30 will be set to 0.
		* _CoDcorrect_ gets the death data using the shared function _get_draws_, from it takes the mean, 75th percentile and 25 th percentile
	to calculate the coefficient of variation for the death data. Death data is pulled for all locations (746), HF etiologies (21), all DisMod model
	age groups (0 through 95+ in 5 year increments), both sexes and all years. If not all those age groups are given then a composite age group
	e.g 80+ is infered for every 5 year increment from 80 to 95+. The dataset is collapsed for all years (death counts are summed up for all years),
	so there is only columns age, sex, location, mean, upper, and lower -- until coefficient of variation is calculated.
		* _MarketScan_DeathData_ combines the marketscan data with death data, merging on age, sex, and cause. Therefore the Marketscan is inferred from
		the US to all other locations, which the death data provides. Then the proportion of HF due to each etiology for each age, sex, and location
		combo is calculated. Also using the coefficient of variation from both the Marketscan and the death data, a third coefficient of variation
		for the proportions is calculated
	3. _01_get_draws_subprocess.py_ -- the script that runs in parallel (by etiology and location) to retrieve all the death data.
	the get_draws query's arguments will need to changed for example the gbd_round_id and version/status argument calls will need to change.
	4. _hf_functions_ -- some other custom functions used by the program.
	5. _upload_hf_mktscan_prep.py_ -- preps the proportions for upload to the database.
		* drop sub-saharan Africa estimates
		* take six main/composite etiologies to be uploaded to six respective bundle IDs
		* add necessary columns to data files (in excel format as is required by the epi-uploader)
	6. _upload_it_ -- does the actual uploading. runs in parallel for the 6 bundles, called either by (1) or (5).

# To run program: Make sure your in folder "old_HF_cc_marketscan_props" and logged on to the cluster (I would use 5 or 10 slots), then enter:
	python 00_prep_hf_mktscan_parallel.py

+ The program will prompt to answer some questions:
	1. "What's your username (for qsub errors/outputs)?" -- it uses this to make the file paths for qsub errors/outputs
	2. "Do you want to Slack a person/channel when the prog finisher?" -- answer yes if you want to get slack messages when the program finishes
		   Note: if there are missing is missing data from the "get_draws" parallelized queries then it'll send you a slack warning there too.
		   It will also send you a slack message when each of the six etiology-splits get uploaded to the Epi-database
	3."#channel or @person to send message: " -- enter either a slack channel or a handle, including the "#" or "@" 
	4. "Do you need to get new CODcorrect data: " -- enter "yes" if new CoDcorrect data is needed otherwise it uses the data from the file
		   already written
	5. "Enter 'upload' to upload the data or 'validate' to validate the data for upload: " -- just put "upload". When I was first setting up this code
		   the upload would fail, if I didn't have set up just right, so validations are safe/quicker way to check without
		   trying to upload something that won't work.
	

# hf_input_estimates.R
  + Purpose:   Shows how heart failure eiologies-splits over age/sex/location. Produced with MarketScan and CODcorrect. These are inputs for DisMod.
  + you'll need to be on the shiny server for this
	

	
	