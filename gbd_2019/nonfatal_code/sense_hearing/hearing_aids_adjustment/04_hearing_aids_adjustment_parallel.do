** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USER
// Description:	apply severity specific hearing aids prevalences to adjust severity-specific parent prevalence

** run: do "FILEPATH"
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
//Running interactively on cluster
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by USER, available from: reference: ADDRESS
local username "`c(username)'"
local decomp "step4"

** do "FILEPATH"
	local cluster_check 0
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2018_06_13"
		local 4		"04"
		local 5		"hearing_aids_adjustment"
		local 6		"FILEPATH"
		local 7		"58"
		}

// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		//adopath + "FILEPATH"

	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "04"
		local step_name "hearing_aids_adjustment"
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
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
			//USER: hardcoded for now... model_custom doesn't pass it, rather by default tells us to 
			//make an input folder within root_j_dir, but I don't want to have separate input folders
			//in both FILEPATH and FILEPATH, so I'll use the input folder structure from 2013 
		local in_dir "FILEPATH"
		// directory for output on the J drive
		cap mkdir "`root_j_dir'"
		cap mkdir "`root_j_dir'/FILEPATH"
		cap mkdir "`root_j_dir'/FILEPATH"
		cap mkdir "`root_j_dir'/FILEPATH"
		local out_dir "`root_j_dir'/FILEPATH"
		// directory for output on clustertmp
		cap mkdir "`root_tmp_dir'"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		cap mkdir "`root_tmp_dir'/FILEPATH"
		local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	//Note USER: right now I have qsub set to only write outputs and errors if in diagnostic mode to reduce files (if re-adding, may need to close log at end)
		
		// write log if running in parallel and log is not already open
		cap mkdir "`out_dir'/FILEPATH"
		cap mkdir "`out_dir'/FILEPATH"
		cap log using "`out_dir'/FILEPATH", replace
		if !_rc local close_log 1
		else local close_log 0
		
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2015 2017 2019"
		//local year_ids "2017 2019"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {

		** bring in proportion of people with hearing loss with hearing aids 
		use "FILEPATH", clear 
		keep age_group_id sex_id aids* //drop vars that needed to be kept for diagnostics
		tempfile aids
		save `aids', replace

		** bring in prevalence information
		use "FILEPATH", clear 
		drop *db* //drop vars that needed to be kept for diagnostics
		tempfile envelope
		save `envelope', replace


	use `aids', clear
	merge 1:1 age_group_id sex_id using `envelope', nogen
	tempfile working_temp
	save `working_temp'
	
	** make a place to save adjusted information
	keep age_group_id sex_id
	tempfile adjusted_temp
	save `adjusted_temp', replace

	** adjust hearing envelope with hearing aids data (if you have hearing aids you get shifted down a severity level...)
		** ex: if 40% 65-80 dB have hearing aids, those people will go into the 50- 65 dB
		** Note: hearing aid use is given as proportion
		** Note USER: I'm not sure this is the most computationally efficient loop, but I'll go with it 

		local draw 0
		quietly {
			forvalues draw=0/999 {

				noisily di in red "DRAW `draw'! `step_name' - `year_id' sex `sex_id'"
				
				use `working_temp', replace

				** category by category apply the adjustment
					
					** convert hearing aid coverage to prevalence (severity prevalence * hearing aid coverage)
						local sev 20
						forvalues sev=20(15)95 {
							gen prev_aids_sev_`sev'_draw_`draw' = draw_`draw'_`sev' * aids_sev_`sev'_draw_`draw'
						}					
					
					** make sure hearing aid coverage is never over 95% for a given severity and 0 if missing (birth prevalence)
						local sev 20
						forvalues sev=20(15)95 {
							gen prev_95pct_`sev'= draw_`draw'_`sev' * .95
							replace prev_aids_sev_`sev'_draw_`draw' = prev_95pct_`sev' if prev_aids_sev_`sev'_draw_`draw' > prev_95pct_`sev' & prev_aids_sev_`sev'_draw_`draw' != .
							replace prev_aids_sev_`sev'_draw_`draw' = 0 if age_group_id==164
							if prev_aids_sev_`sev'_draw_`draw' == . {
								display as error "missing values in hearing aid prevalence"
								exit 111
							}
						}

					** 20-35 (people with hearing aids at 20-35 just get dropped out)
						gen adjusted_`draw'_20 = draw_`draw'_20 - prev_aids_sev_20_draw_`draw'
				
					** 35-50
						gen adjusted_`draw'_35 = draw_`draw'_35 - prev_aids_sev_35_draw_`draw'
						replace adjusted_`draw'_20 = adjusted_`draw'_20 + prev_aids_sev_35_draw_`draw'
					
					** 50-65
						gen adjusted_`draw'_50 = draw_`draw'_50 - prev_aids_sev_50_draw_`draw'
						replace adjusted_`draw'_35 = adjusted_`draw'_35 + prev_aids_sev_50_draw_`draw'			
					
					** 65-80
						gen adjusted_`draw'_65 = draw_`draw'_65 - prev_aids_sev_65_draw_`draw'
						replace adjusted_`draw'_50 = adjusted_`draw'_50 + prev_aids_sev_65_draw_`draw'	

					** 80-95  (AS OF 2017 NOT BEING ADJUSTED)
						gen adjusted_`draw'_80 = draw_`draw'_80 // - prev_aids_sev_80_draw_`draw'
						* replace adjusted_`draw'_65 = adjusted_`draw'_65 + prev_aids_sev_80_draw_`draw'	

					** 95+ (note USER: there is no adjustment for 95+)
						gen adjusted_`draw'_95 = draw_`draw'_95 // - prev_aids_sev_95_draw_`draw'
						* replace adjusted_`draw'_80 = adjusted_`draw'_80 + prev_aids_sev_95_draw_`draw'	
				
				// drop *draw_`draw'_* *draw_`draw' prev_95pct* prev_aids* do NOT want to drop these variables, want them to be saved to be used in diagnostics 
					drop prev_95pct* //because not draw specific
				save `working_temp', replace
					
			}
		}
		
		use `working_temp', clear
		
	** save draws in intermediate location
		format adj* %16.0g
		cap mkdir "FILEPATH"
		save "FILEPATH", replace
		count

	** save summary in intermediate location
		** forvalues s=20(15)65 {
			** egen double mean_`s' = rowmean(adjusted_`s'_*)
			** egen double upper_`s' = rowpctile(adjusted_`s'_*), p(97.5)
			** egen double lower_`s' = rowpctile(adjusted_`s'_*), p(2.5)
		** }
		** drop *adj*
		** gen iso3 = "`iso3'"
		** gen year = `year'
		** gen sex = "`sex'"
		** format mean* upper* lower* %16.0g
		** compress
		** cap mkdir "FILEPATH"
		** outsheet using "FILEPATH", comma replace
		


		//Next sex	
		}
	//Next year 
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

cap log close //Note USER: this is superfluous if I'm not actually writing logs in parallel jobs 

	// write check file to indicate sub-step has finished
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		file open finished using "FILEPATH", replace write
		file close finished
