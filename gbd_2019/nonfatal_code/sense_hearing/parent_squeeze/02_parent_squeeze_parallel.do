// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
** Purpose:		This sub-step template is for parallelized jobs submitted from main step code
** Author:		USER
** Description:	squeeze parent hearing envelope with discrete categories (0-19 dB, 20-34 dB, 35-49 dB, 50-64 dB, 65-79 dB, 80-94 dB, 95+ dB) and 35+ info
			** 1) 35+ information needs to be incorporated.  since we are more certain about these estimates this is reflected in the squeeze at the draw level.
			** 2) squeeze discrete categories into the 35+ 
		** result: parent prevalence squeezed draws
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by Chamara, available from: reference: https://www.stata.com/statalist/archive/2013-10/msg00627.html
local username "`c(username)'"
local decomp "step4"

noisily di "starting step2 child script"
//Running interactively on cluster 
** do "FILEPATH"
	local cluster_check 0
	
	if `cluster_check' == 1 {
		local 1		"FILEPATH
		local 2		"FILEPATH"
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr(`"`date'"'," ","_",.)
		local 4		"02"
		local 5		"parent_squeeze"
		local 6		"FILEPATH"
		local 7		"58"
		}
	
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
	clear all
	set more off
	set mem 2g
	set maxvar 32767
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		local cluster 0
	}
	// directory for standard code files

	run "FILEPATH"
	run "FILEPATH"
	


	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "02"
		local step_name "parent_squeeze"
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
		local location_id "ID"

		}

	//If running on cluster, use locals passed in by model_custom's qsub
	else if `cluster' == 1 {
		// base directory on scratch 
		local root_j_dir `1'
		// base directory on cluster
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
		// directory for output onto share scratch
		cap noisily mkdir "`root_j_dir'"
		cap noisily mkdir "`root_j_dir'/FILEPATH"
		cap noisily mkdir "`root_j_dir'/FILEPATH"
		cap noisily mkdir "`root_j_dir'/FILEPATH'"
		local out_dir "FILEPATH"
		// directory for output on clustertmp
		cap noisily mkdir "`root_tmp_dir'"
		cap noisily mkdir "`root_tmp_dir'/FILEPATH"
		cap noisily mkdir "`root_tmp_dir'/FILEPATH"
		cap noisily mkdir "`root_tmp_dir'/FILEPATH"
		local tmp_dir "`root_tmp_dir'/FILEPATH"

	//Note USER: right now I have qsub set to only write outputs and errors if in diagnostic mode to reduce files (if re-adding, may need to close log at end)
		
		// write log if running in parallel and log is not already open
		cap noisily log using "FILEPATH", replace
		if !_rc local close_log 1
		else local close_log 0
		
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

	//Define age_group_ids to run on 
	local age_ids_list "ID" 
	
	//Get num of required ages (this could likely be more efficient)
		clear 
		set obs 1
		gen ages = "`age_ids_list'"
		split ages, p(",")
		tokenize "`age_ids_list'", p(",")
		local num_age_ids = `r(nvars)'

	//Hearing dimensions (in dB) 
		** 2788 0-19 
		** 2629 20-34
		** 2406 35+
			* 2630 35-49
			* 2631 50-64
			* 2632 65-79
			* 2633 80-94
			* 2410 95+


		** load DisMod draws for hearing loss envelopes 
		local meids "2788 2629 2406 2630 2631 2632 2633 2410"
		//see which models are marked best (this is not used in code, just for diagnostics)
			if `cluster' { 
				get_best_model_versions, entity(modelable_entity) ids(`meids') decomp_step("`decomp'") clear
				tab modelable_entity_id
				count 
				if `r(N)' != 8 di in red "NOT ALL MODELS HAVE BEST VERSION" BREAK 
				}

		foreach healthstate in _hearing_0_19db _hearing_20_34db _hearing_35db _hearing_35_49db _hearing_50_64db _hearing_65_79db _hearing_80_94db _hearing_95db  { 
		   if "`healthstate'" == "_hearing_0_19db" local meid 2788 
		   if "`healthstate'" == "_hearing_20_34db" local meid 2629
		   if "`healthstate'" == "_hearing_35db" local meid 2406
		   if "`healthstate'" == "_hearing_35_49db" local meid 2630
		   if "`healthstate'" == "_hearing_50_64db" local meid 2631
		   if "`healthstate'" == "_hearing_65_79db" local meid 2632
		   if "`healthstate'" == "_hearing_80_94db" local meid 2633
		   if "`healthstate'" == "_hearing_95db" local meid 2410

		   	di "" _new "***" _new "" _new "LOADING DISMOD DRAWS FOR MEID `meid' HEALTHSTATE `healthstate' LOCATION `location_id'" _new "" _new "***"
		   
		   	//load prevalence draws
		 		  	get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(5) location_id(`location_id') decomp_step("`decomp'") clear
					keep if inlist(age_group_id, `age_ids_list')

					//Make sure all required ages are present 
					tab age_group_id
					if `r(r)' != `num_age_ids' di in red "MEID `meid' MISSING 1 OR MORE AGE_GROUP_IDS" BREAK

					//new line of code to convert 0-19 HL back to normal prevalence
					if `meid' == 2788 forvalues i=0/999 {
						replace draw_`i' = 1-draw_`i'
						} 


			//save temp file 
				tempfile `healthstate'_draws 
				save ``healthstate'_draws', replace
			}
		

//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2015 2017 2019"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {


		** run whatever transformations on draws you need here
		** combine all info into one file
			
			local i 0  

			foreach db in 0_19db 20_34db 35db 35_49db 50_64db 65_79db 80_94db 95db {
				use `_hearing_`db'_draws' if year_id == `year_id' & sex_id == `sex_id', clear
				rename draw* draw*_`db'
				if `i' == 0 tempfile working_temp 
				else merge 1:1 age_group_id using `working_temp', nogen
				save `working_temp', replace
				local ++ i 
				}

	

		** loop through each draw and calculate mutually exclusive, discrete (non-cumulative) prevalence estimates (ex 20-35 db, instead of 20+ dB prevalence)		
		** Perform 2 squeezes
			** 1) 35+ information needs to be incorporated.  since we are more certain about these estimates this is reflected in the squeeze at the draw level.
			** 2) squeeze discrete categories into the 35+ 
		
			local draw 0
			quietly {
				forvalues draw = 0/999 {
					noisily di in red "Draw `draw'!! `step_name' loop 1 ~~~ year `year_id' sex `sex_id'"
		
					use `working_temp', clear		
					
					** (1) squeeze 35+, 0-19, and 20-34 to sum up to 1 We want to incorporate the 35+ model because I have the most information with this (it is the standard definition of hearing loss)
					
						gen total= draw_`draw'_0_19db + draw_`draw'_20_34db + draw_`draw'_35db
						
						foreach db in 0_19db 20_34db 35db {
							gen draw_`draw'_`db'_squeeze1 = draw_`draw'_`db' * (1/total)
						}
						
						drop total						
					
					** (2) squeeze categorical results into 35+ parent envelope (0-19 and 20-35 not a part of the squeeze)
						
						gen total= draw_`draw'_35_49db + draw_`draw'_50_64db + draw_`draw'_65_79db + draw_`draw'_80_94db + draw_`draw'_95db
						

						foreach db in 35_49db 50_64db 65_79db 80_94db 95db {
							gen draw_`draw'_`db'_prop = draw_`draw'_`db' * (1/total)
							gen draw_`draw'_`db'_squeeze2 = draw_`draw'_`db'_prop * draw_`draw'_35db_squeeze1
							}
						
						drop total						
					
					** rename prevalence estimates for the categories of interest
						gen draw_`draw'_20 = draw_`draw'_20_34db_squeeze1
						gen draw_`draw'_35 = draw_`draw'_35_49db_squeeze2
						gen draw_`draw'_50 = draw_`draw'_50_64db_squeeze2
						gen draw_`draw'_65 = draw_`draw'_65_79db_squeeze2
						gen draw_`draw'_80 = draw_`draw'_80_94db_squeeze2
						gen draw_`draw'_95 = draw_`draw'_95db_squeeze2
					
						//note USER: the superfluous variables are kept right now for diagnostics, they will be dropped in step 04
					
					save `working_temp', replace
				}
			}
			
			use `working_temp', replace

			
		** save draws in intermediate location
			format draw* %16.0g
			cap mkdir "FILEPATH"
			save "FILEPATH", replace
			
		** ** calculate and summary
			** forvalues s=20(15)65 {
				** egen double mean_`s' = rowmean(draw*`s')
				** egen double upper_`s' = rowpctile(draw*`s'), p(97.5)
				** egen double lower_`s' = rowpctile(draw*`s'), p(2.5)
			** }
			
			** drop draw*
			** gen iso3 = "`iso3'"
			** gen year = `year'
			** gen sex = "`sex'"
			** format mean* upper* lower* %16.0g
			** compress
			** save "FILEPATH", replace			
		
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
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		file open finished using "FILEPATH", replace write
		file close finished
