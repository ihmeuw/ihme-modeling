// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
** Purpose:		This sub-step template is for parallelized jobs submitted from main step code
** Author:		USER
** Description:	make prevalence estimates by etiology

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by Chamara, available from: reference: https://www.stata.com/statalist/archive/2013-10/msg00627.html
local username "`c(username)'"
local decomp "step4"

** do "FILEPATH"
	local cluster_check 0
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2019_10_06"
		local 4		"05"
		local 5		"etiology_squeeze"
		local 6		"FILEPATH" 
		local 7		"43939"  
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
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		adopath + "FILEPATH"


	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "05"
		local step_name "etiology_squeeze"
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
			//in both J:/temp and J:/work, so I'll use the input folder structure from 2013 
		local in_dir "FILEPATH"
		// directory for output on the J drive
		cap mkdir "`root_j_dir'"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"		
		cap noisily mkdir "FILEPATH"
		local out_dir "FILEPATH"
		// directory for output on clustertmp
		//cap noisily mkdir "FILEPATH"
		//cap noisily mkdir "FILEPATH"
		//cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		cap noisily mkdir "FILEPATH"
		local tmp_dir "FILEPATH"
	
	//Note USER: right now I have qsub set to only write outputs and errors if in diagnostic mode to reduce files (if re-adding, may need to close log at end)
		/*
		// write log if running in parallel and log is not already open
		cap log using "FILEPATH", replace
		if !_rc local close_log 1
		else local close_log 0
		*/
	run "FILEPATH"
	run "FILEPATH"
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
local age_ids_list "ID" 

	//Get num of required ages (this could likely be more efficient)
	clear 
	set obs 1
	gen ages = "`age_ids_list'"
	split ages, p(",")
	tokenize "`age_ids_list'", p(",")
	local num_age_ids = `r(nvars)'

	**********************************
	** LOAD DISMOD RESULTS
	**********************************
		
		//INPUT ETIOLOGY MEIDS
			*1282 Chronic otitis media
			*2311 Age-related and other hearing loss unsqueezed (PROPORTION MODEL)
			*24049 Hearing loss due to meningitis unsqueezed
		*local meids "1282 2311 24049"
		import excel using "FILEPATH", clear firstrow 
		levelsof modelable_entity_id, local(meids) 
		count 
		local num_meids = `r(N)'

		//see which models are marked best 
		/*
			get_best_model_versions, entity(modelable_entity) ids(`meids') decomp_step("step2") clear
			levelsof modelable_entity_id, local(meids_best) clean sep(",")
			count 
			local num_meids_best = `r(N)'
			local n_miss = `num_meids' - `num_meids_best'
			if `n_miss' > 0 {
				di "NECESSARY MEIDS: `meids'"
				di "MEIDS MARKED BEST:"
				levelsof modelable_entity_id
				di in red "MISSING `n_miss' BEST MODELS - WILL FILL WITH ZERO" 
				sleep 5000
				}
`*/
		foreach meid in `meids' {
			//noisily di "LOAD DRAWS FROM MEID `meid' - `name'"
			noisily di "LOAD DRAWS FROM MEID `meid'"

			if `meid' == 1282 get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(5) location_id(`location_id') decomp_step("step4") gbd_round_id(6) clear
			if `meid' == 2311 get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(18) location_id(`location_id') decomp_step("step4") gbd_round_id(6) clear
			if `meid' == 24049 get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(5) location_id(`location_id') decomp_step("step4") gbd_round_id(6) clear

			//If marked best, load get_draws 
			/*
			if inlist(`meid', `meids_best') {
					noisily di "LOAD DRAWS FROM MEID `meid' - `name'"
			   		 //Load 2311 as proportion (because that's how it's modeled in Dismod)
					if `meid' == 2311 get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(18) location_id(`location_id') decomp_step("step2") gbd_round_id(6) clear

					//Load rest as prevalence 
					else get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`meid') measure_id(5) location_id(`location_id') decomp_step("step2") gbd_round_id(6) clear
			   		}
			   		*/

			//If no best model, load zeros 
			/*
			if !inlist(`meid', `meids_best') {
			   		noisily di in red "NO MODEL MARKED BEST FOR MEID `meid' - `name' - LOADING ZEROES"
					if `cluster' == 1 get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(2406) measure_id(5) location_id(`location_id') decomp_step("step2") gbd_round_id(6) clear
					forval i = 0/999 {
						qui replace draw_`i' = 0 
						}
			   		}
			   		*/

			keep if inlist(age_group_id, `age_ids_list')

			//If missing birth prev, impute as neonatal
		   			count if age_group_id == 164
		   			if `r(N)' == 0 {
		   				noisily di in red "Missing birth prev - imputing as neonatal"
		   				expand 2 if age_group_id == 2, gen(dup)
		   				replace age_group_id = 164 if dup == 1 
		   				drop dup 
		   				}

			//Make sure all required ages are present 
				tab age_group_id
				if `r(r)' != `num_age_ids' di in red "MEID `meid' MISSING 1 OR MORE AGE_GROUP_IDS" BREAK 

			//save tempfiles 
			if `meid' == 2311 local cause "other"
			if `meid' == 1282 local cause "otitis"
			if `meid' == 24049 local cause "meningitis"
			

			rename draw* `cause'_draw*
			tempfile `cause'_temp //tempfiles for year-sex specific data (used below)
			tempfile `cause'_dismod
			save ``cause'_dismod', replace 
			}



//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2015 2017 2019"
	//sex_ids
		local sex_ids "1 2"

	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {

	//Pull in hearing-aid adjusted severity levels from previous step. Inputs from step 4. USER: for report, modifying to pull envelopes from step 2.
	//use "FILEPATH", clear 
	use "FILEPATH", clear 

	keep age_group_id draw* // Others were kept for diagnostics
	drop *db* //drop vars that needed to be kept for diagnostics

	tempfile envelope
	save `envelope', replace
		
	**********************************
	** Adjust input etiologies such that it is prevalence of hearing loss due to x etiology by severity
	**********************************

	***AGE-RELATED/ OTHER HEARING LOSS 
		// turn other hearing loss from proportion into prevalence by severity (assume proportion same across all severities)
			**note USER: it seems a little odd that we're splitting according to the total prevalence splits, which include otitis (which we know is only in mild and moderate)
			use `other_dismod' if year_id == `year_id' & sex_id == `sex_id', clear
			merge 1:1 age_group_id using `envelope', nogen
			
			//Prevalence of other hearing loss at given severity = proportion of other hearing loss * (hearing aid adjusted) prevalence at given severity 
			forvalues sev=20(15)95 {
				forvalues draw=0/999 {
					gen other_sev`sev'_draw_`draw' = other_draw_`draw' * draw_`draw'_`sev'
				}
			}
			
			drop other_draw* draw*
			
			tempfile other_temp
			save `other_temp', replace
				
	*** OTITIS  (coming in already as prevalence)
		// Chronic otitis media only leads to mild and moderate hearing loss 
				//0 prevalence for categories above moderate
				// Mild and moderate split proportionally as per Fria 1985 Figure 2 (FILEPATH due to otitis media)
				
			clear
			set obs 1000
			gen x = rbeta(540*0.8, 540*0.2)
			xpose, clear
			//Generate 1000 draws of the proportion of hearing loss due to otitis that is mild 
			forvalues num = 1/1000 {
				local draw = `num' - 1
				rename v`num' prop_mild_`draw'
				}
			gen om = "om"
			tempfile prop_mild
			save `prop_mild', replace
			clear			
			
			use `otitis_dismod' if year_id == `year_id' & sex_id == `sex_id', clear
			gen om = "om"
			merge m:m om using `prop_mild', keep(1 3) nogen

			forvalues draw=0/999 {
				//Calculate prevalence of mild and moderate hearing loss due to otitis
				gen otitis_sev20_draw_`draw' = otitis_draw_`draw' * prop_mild_`draw'
				gen otitis_sev35_draw_`draw' = otitis_draw_`draw' * (1 - prop_mild_`draw')
				//Other severities have zero prevalence 
				forvalues sev=50(15)95 {
					gen otitis_sev`sev'_draw_`draw' = 0
					}
				}			
			
			drop otitis_draw* prop_mild*
			
			save `otitis_temp', replace
			

	*** MENINGIITS 
		// menigitis is coming in as 26+ db rather than 35+ db estimates.  crosswalk using nhanes data
			**STRATEGY (updated 1/20/2016) : as per Theo's request, we crosswalk 26+ to 35+, then apply the remainder to 20-34. 
		** This crosswalk file was created with this code: "FILEPATH"
			** local acause meningitis_pneumo
			//foreach acause in meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other
			foreach acause in meningitis {
				use ``acause'_dismod' if year_id == `year_id' & sex_id == `sex_id', clear
				merge 1:1 _n using "`in_dir'/crosswalks_map_for_meningitis.dta", nogen
				forvalues draw=0/999 {
					replace crosswalk`draw' = crosswalk`draw'[1]
					gen `acause'_35_draw_`draw' = `acause'_draw_`draw' * crosswalk`draw'
					gen `acause'_2034_draw_`draw' = `acause'_draw_`draw' * (1 - crosswalk`draw')
					}	
				drop crosswalk* `acause'_draw*
				save ``acause'_temp', replace
				}
		
		// proportionally split meningitis prevalences by the envelope (not squeezed yet)
			
			use `meningitis_temp', clear

			merge 1:1 age_group_id using `envelope', nogen
			
			rename meningitis* meng*
			
			tempfile meng_temp
			save `meng_temp', replace
			
			forvalues draw=0/999 {
				di in red "draw `draw' for meng loop"
				//foreach acause in meng_pneumo meng_hib meng_meningo meng_other
				foreach acause in meng {
					
					//Total hearing loss envelope (all severities, all causes)
					egen total = rowtotal(draw_`draw'*)
					
					//Calculate each severity's proportion of all hearing loss (and apply to meningitis at given severity)
					forvalues sev=20(15)95 {
						gen prop_`sev' = draw_`draw'_`sev' / total
						// 20-34 meningitis drawn directly from crosswalk remainder 
						if `sev' == 20 gen `acause'_sev`sev'_draw_`draw' = `acause'_2034_draw_`draw'
						//35+ categories calculated by proportion of that severity * prevalence of 35+ meningitis 
						else 		   gen `acause'_sev`sev'_draw_`draw' = prop_`sev' * `acause'_35_draw_`draw'
						}
					drop prop* total `acause'_35_draw_`draw' `acause'_2034_draw_`draw'
				}
				
				drop draw_`draw'_*
			}

			save `meng_temp', replace			
			
**********************************
** Start Squeeze Process
**********************************		

		** 1) Prevalence at birth assumed to be all congenital.  prevalence remains constant (no excess mortality or remision)
		** 2) Squeeze congenital, other, otitis, and all meningitis ONLY UP TO AGE 20 (the "age related and other" cause only has reliable data for ages < 20)
		** 3) Stream out congenital over all ages again (since it got adjusted during squeeze)
		** 4) For all ages (remmember, <20 has been squeezed, but may not be quite at level of envelope due to second congenital adjustment), assign "age related and other" hearing loss as remainder between sum and envelop 
		
		forvalues sev=20(15)95 {
			use `envelope', clear
			keep age_group_id draw*`sev'
			merge 1:1 age_group_id using `other_temp', nogen keepusing(*sev`sev'*)
			merge 1:1 age_group_id using `otitis_temp', nogen keepusing(*sev`sev'*)
			merge 1:1 age_group_id using `meng_temp', nogen keepusing(*sev`sev'*)
	
			tempfile working_sev`sev'
			save `working_sev`sev'', replace
			}
	
		** SQUEEZE!
				
				** 1) first make it so congenital prevalence is constant based off of birth prevalence 
				**	local draw 0
				**	local sev 95
					quietly {
						
						forvalues sev=20(15)95 {
							
							use `working_sev`sev'', clear
							
							forvalues draw=0/999 {
								di in red "DRAW `draw'! sev `sev', make congenital loop"	
							
								summ draw_`draw'_`sev' if age_group_id==2 //neonatal
								gen cong_other_sev`sev'_draw_`draw' = `r(mean)'
								
							}
						
							save `working_sev`sev'', replace

							//Save an intermediate file - this is ONLY used in diagnostics, is not used for any calculations
							cap mkdir "FILEPATH"
							cap mkdir "FILEPATH"
							save "FILEPATH", replace
						}
					}

					

				** 2) do the squeeze ONLY UP TO AGE 20		
					quietly {
						
					**	local sev 20
					**	local draw 0
						forvalues sev=20(15)95 {
							
							use `working_sev`sev'', clear
							
							** keep *_0_* *_0
							
							forvalues draw=0/999 {
								
								noisily di in red "DRAW `draw'! sev `sev', squeeze"						
								
								// squeeze the all etiologies into the envelope
									gen total = otitis_sev`sev'_draw_`draw' + meng_sev`sev'_draw_`draw'  + other_sev`sev'_draw_`draw' + cong_other_sev`sev'_draw_`draw'
							
								foreach etiology in otitis cong_other other meng {
									gen proportion_`etiology' = `etiology'_sev`sev'_draw_`draw' / total
									replace `etiology'_sev`sev'_draw_`draw' = draw_`draw'_`sev' * proportion_`etiology' if age_group_id <= 8 // under 20 
									drop proportion_`etiology'
								}
								drop total 	//Keep adjusted vars for diagnostics (dropped later) 
							}
							
							save `working_sev`sev'', replace						
						}					
					}

	
				** 3) Make it so congenital prevalence is constant based off of birth prevalence (may have been altered during squeeze)
				**	local draw 0
				**	local sev 95
					quietly {
						
						forvalues sev=20(15)95 {
							
							use `working_sev`sev'', clear
							
							forvalues draw=0/999 {
								di in red "DRAW `draw'! sev `sev', make second congenital loop"	
							
								summ cong_other_sev`sev'_draw_`draw' if age_group_id==2 //neonatal
								replace cong_other_sev`sev'_draw_`draw' = `r(mean)'
				
							}
						
							save `working_sev`sev'', replace

						}
					}


				** 4) For all ages (remmember, <20 has been squeezed, but may not be quite at level of envelope due to second congenital adjustment), assign "age related and other" hearing loss as remainder between sum and envelop 
				**	local draw 0
				**	local sev 95
					quietly {
						
						forvalues sev=20(15)95 {

local error_dx_`sev' 0 
							use `working_sev`sev'', clear
							
							forvalues draw=0/999 {
								di in red "DRAW `draw'! sev `sev', assign remainder"	
							
								// Sum all etiologies (except "age related and other")
								gen total = otitis_sev`sev'_draw_`draw' + meng_sev`sev'_draw_`draw'  + cong_other_sev`sev'_draw_`draw'
								
								// Apply remainder of etiology sum and envelope as "age related and other"
								// TODO: Check out the logic in the line below. The line below is a likely candidate for where negative draws could be occurring
								replace other_sev`sev'_draw_`draw' = draw_`draw'_`sev' - total								



	count if other_sev`sev'_draw_`draw' < 0 
	if `r(N)' >= 1 {
		local error_dx_`sev' 1 
		replace other_sev`sev'_draw_`draw' = 0 if other_sev`sev'_draw_`draw' < 0 
		}


								drop total 
							}	//next draw 
if 0 == 0 {
				
if `error_dx_`sev'' == 1 {
	file open error_dx using "FILEPATH", replace write
	file close error_dx
	}
	}

	//Save an intermediate file. Output filepath. Changing to a world hearing report directory.
	save "FILEPATH", replace 
					

			} //next sev 

			} // end quietly


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
