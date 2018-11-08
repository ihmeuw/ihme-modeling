** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** Purpose:		This sub-step template is for parallelized jobs submitted from main step code
** Description:	get prevalence of hearing aid use in each severity
** Run: do FILEPATH
		
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by Chamara, available from: reference: https://www.stata.com/statalist/archive/2013-10/msg00627.html
local username "`c(username)'"

//Running interactively on cluster
** do FILEPATH
	local cluster_check 0
	if `cluster_check' == 1 {
		local 1		FILEPATH
		local 2		FILEPATH
		local 3		"2018_05_16"
		local 4		"03"
		local 5		FILEPATH
		local 6		FILEPATH
		local 7		"58"
		}
	
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix FILEPATH
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix FILEPATH
		local cluster 0
	}
	// directory for standard code files
		adopath + FILEPATH

	//If running locally, manually set locals
	if `cluster' == 0 {

		//local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        //local date = subinstr("`date'", " " , "_", .)
        local date = "2018_05_08"
		local step_num "03"
		local step_name FILEPATH
		local code_dir FILEPATH
		//local in_dir FILEPATH
		local root_j_dir FILEPATH
		local root_tmp_dir FILEPATH
		local location_id "58"

		}

	//If running on cluster, use locals passed in by model_custom's qsub
	else if `cluster' == 1 {
		// base directory on J 
		local root_j_dir `1'
		// base directory on clustertmp
		local root_tmp_dir `2'
		// timestamp of current run (i.e. 2014_01_17) 
		local date `3'
		// step number of this step (i.e. 01a)
		local step_num `4'
		// name of current step (i.e. first_step_name)
		local step_name `5'
		// directory for steps code
		local code_dir `6'
		local location_id `7'

		}
	
	**Define directories 
		// directory for external inputs
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		local out_dir FILEPATH
		// directory for output on clustertmp
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		local tmp_dir FILEPATH
	
		/*
		// write log if running in parallel and log is not already open
		cap log using FILEPATH, replace
		if !_rc local close_log 1
		else local close_log 0
		*/
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
	//Hearing dimensions (in dB) 
		** 2788 0-19 
		** 2629 20-34
		** 2406 35+
			* 2630 35-49
			* 2631 50-64
			* 2632 65-79
			* 2633 80-94
			* 2410 95+

	//STRATEGY: 
		//Country X severity specific hearing aid coverage = (country X hearing aid coverage / Norway hearing aid coverage) * Norway severity specific hearing aid coverage 
		*Remember: this is in terms of coverage, not prevalence. Ie, outcome is of people with a given hearing loss severity, what proportion have hearing aids. Step 4 converts this to a prevalence 

	*************************************
	** LOAD INFO
	*************************************
	get_age_metadata, age_group_set_id(12) clear
	levelsof age_group_id, local(age_ids_list) clean
	//load hearing aid coverage draws for Norway
			get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(2411) measure_id(5) age_group_id(`age_ids_list') location_id(90) clear
			rename draw* Norway_draw*
			tempfile Norway_hearing_aid_coverage
			save `Norway_hearing_aid_coverage', replace 
	//load hearing aid coverage draws for country of interest
  		  	get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(2411) measure_id(5) age_group_id(`age_ids_list') location_id(`location_id') clear
 		  	tempfile country_hearing_aid_coverage
			save `country_hearing_aid_coverage', replace 


	*************************************
	** CALCULATE RATIO
	*************************************
		merge 1:1 year_id sex_id age_group_id using `Norway_hearing_aid_coverage', nogen 
			forvalues draw = 0/999 {
				gen ratio_draw_`draw' = draw_`draw' / Norway_draw_`draw'
				//drop Norway_draw_`draw'
				}
			save `country_hearing_aid_coverage', replace 

//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2017"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {


		use `country_hearing_aid_coverage' if year_id == `year_id' & sex_id == `sex_id', clear 

		//Call in severity specific hearing aid coverage (from regression in parent code)
		forvalues sev=20(15)80 {
			merge 1:1 age_group_id sex_id using FILEPATH, keep(3) nogen
			}

			
			//Country X severity specific hearing aid coverage = (country X hearing aid coverage / Norway hearing aid coverage) * Norway severity specific hearing aid coverage 
			local draw 0	
				forvalues draw = 0/999 {
						di in red "Draw `draw'! `step_name'"
						
						forvalues sev=20(15)80 {
							gen aids_sev_`sev'_draw_`draw' = ratio_draw_`draw' * sev_`sev'_`draw'
						}
						// Assume no correction for deafness
						gen aids_sev_95_draw_`draw'=0

				}		
				
				


	*************************************
	** SAVE INFO
	*************************************	
		** save draws in intermediate location
			format *draw* %16.0g
			cap mkdir FILEPATH
			save FILEPATH, replace
			
		if 0 == 1 {
		** calculate and summary
			 *local s 65
			 forvalues s=20(15)95 {
				 egen double mean_`s' = rowmean(aids_sev_`s'_draw*)
				 egen double upper_`s' = rowpctile(aids_sev_`s'_draw*), p(97.5)
				 egen double lower_`s' = rowpctile(aids_sev_`s'_draw*), p(2.5)
			 }
			 drop *draw*
			 format mean* upper* lower* %16.0g
			 cap gen location_id = `location_id'
			 cap gen year = `year_id'
			 cap gen sex = `sex_id'
			 compress
			 save FILEPATH, replace
			}

		//Next sex	
		}
	//Next year 
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

cap log close 

	// write check file to indicate sub-step has finished
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		cap mkdir FILEPATH
		file open finished using FILEPATH, replace write
		file close finished
