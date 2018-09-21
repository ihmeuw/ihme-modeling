

		local root_j_dir `1'
		local root_tmp_dir `2'
		local date `3'
		local step_num `4'
		local step_name `5'
		local code_dir `6'
		local location_id `7'

	
//Define age_group_ids to run on 
local age_ids_list "2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,164,235" 
	
	//Load envelopes 
	insheet using "FILEPATH", clear
			levelsof modelable_entity_id if type == "pres", local(meids_pres) clean
			tempfile vision_envelope_codebook
			save `vision_envelope_codebook' 

		** load DisMod draws for vision envelopes 
		foreach meid in `meids_pres' {
			use `vision_envelope_codebook' if modelable_entity_id == `meid', clear 
			levelsof type, local(type) clean
			levelsof sev, local(sev) clean

			di "" _new "***" _new "" _new "LOADING DISMOD DRAWS FOR `type' `sev'" _new "" _new "***"

		 	if `cluster' == 1 get_draws, gbd_id_field(modelable_entity_id) source(epi) gbd_id(`meid') measure_ids(5) location_ids(`location_id') clear
			
				***DEVELOPMENT
		   			if `cluster' == 1 & `cluster_check' == 1 cap mkdir "`out_dir'"
		   			if `cluster' == 1 & `cluster_check' == 1 save "`out_dir'/get_data_`sev'", replace 
		   			if `cluster' == 0 use "`out_dir'/get_data_`sev'", clear 
			
			keep if inlist(age_group_id, `age_ids_list')

			tempfile `sev'_envelope_all
			save ``sev'_envelope_all'
			}


// Get list of modelable_entity_id that will be squeezed 
				insheet using FILEPATH clear
				drop if modelable_entity_id == 2424
				levelsof modelable_entity_id, local(meids) clean
				levelsof variable_name, local(names) clean
				count 
				local num_meids = `r(N)'

				
		   	quietly {
		   	foreach meid in `meids' {
			   		
			   		//Name meids 
			   		insheet using FILEPATH, clear
			   		
			   		levelsof variable_name if modelable_entity_id == `meid', local(name) clean
			   		levelsof measure_id if modelable_entity_id == `meid', local(measure_id) clean 

			   	noisily di "LOAD DRAWS FROM MEID `meid' - `name'"
			   	if `cluster' == 1 {
			   		 get_draws, gbd_id_field(modelable_entity_id) source(epi) gbd_id(`meid') measure_ids(`measure_id') location_ids(`location_id') clear
			
			   	}
			   	
		
		   			
			   		gen variable_name = "`name'"
					order variable_name year_id sex_id age_group_id age_group_id draw_*
					sort year_id sex_id age_group_id
		   		tempfile `name'_all
				save ``name'_all', replace
		   		}	//next meid 
		   		}

		
 
//Loop over every year and sex for given location 
	
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2016"
	//sex_ids
		local sex_ids "1 2"



	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {


		//Pull dismod envelope results for each cause, year and sex specific 
		foreach sev in low_mod low_sev blind {	
			use ``sev'_envelope_all'  if year_id == `year_id' & sex_id == `sex_id', clear 
			tempfile `sev'_envelope
			save ``sev'_envelope'
			}


		//Pull dismod cause results for each cause, year and sex specific 
		foreach name in `names' {
			di "`name'"
			use ``name'_all' if year_id == `year_id' & sex_id == `sex_id', clear 
			tempfile `name'_temp
			save ``name'_temp', replace
			}

	
************************************************************************	
** Post hoc adjustment to trachoma and vitamin a: some locations are supposed to have a prevalence of zero.
************************************************************************		
if 1==1 {
	insheet using FILEPATH, clear
	keep if location_id==`location_id'
	
	local low_trach_prev_0 = trach_prev_0
	local blind_trach_prev_0 = trach_prev_0
	local vita_prev_0 = vita_prev_0
		
	// change the tempfiles to reflect this info
	foreach name in blind_trach low_trach vita {
		di in red "`name'"
		use ``name'_temp', clear
	
		foreach var of varlist draw_* {
			qui replace `var' = `var' * ``name'_prev_0'
			}
		
		save ``name'_temp', replace
		}
	}



************************************************************************	
// turn trachoma into prevalence from proportion
************************************************************************			
if 1==1 {
	// for low vision
		use `low_trach_temp', clear

		rename draw_* low_trach_draw_*
		merge 1:1 age_group_id using `low_mod_envelope', nogen
		rename draw_* low_mod_pres_draw_*
		merge 1:1 age_group_id using `low_mod_ref_error_temp', nogen
		rename draw_* low_mod_ref_draw_*
		merge 1:1 age_group_id using `low_sev_envelope', nogen
		rename draw_* low_sev_pres_draw_*
		merge 1:1 age_group_id using `low_sev_ref_error_temp', nogen
		rename draw_* low_sev_ref_draw_*

		
		forvalues c=0/999 {
			replace low_trach_draw_`c' = low_trach_draw_`c' * ((low_mod_pres_draw_`c' - low_mod_ref_draw_`c') + (low_sev_pres_draw_`c' - low_sev_ref_draw_`c'))
			replace low_trach_draw_`c' = 0 if low_trach_draw_`c' < 0 
		}
		
		keep variable_name age_group_id low_trach_draw_*
		rename low_trach_draw_* draw_*
		
		save `low_trach_temp', replace
	
	// for blind
		use `blind_trach_temp', clear

		rename draw_* blind_trach_draw_*
		merge 1:1 age_group_id using `blind_envelope', nogen	
		rename draw_* blind_pres_draw_*
		merge 1:1 age_group_id using `blind_ref_error_temp', nogen
		rename draw_* blind_ref_draw_*
		
		forvalues draw=0/999 {
			replace blind_trach_draw_`draw' = blind_trach_draw_`draw' * (blind_pres_draw_`draw' - blind_ref_draw_`draw')
		}
		
		keep variable_name age_group_id blind_trach_draw_*
		rename blind_trach_draw_* draw_*
		
		save `blind_trach_temp', replace
}

************************************************************************			
// get everything so it is split as low vision moderate, low vision severe, and blind
************************************************************************


	************************************************************************
		** 1. proportionally split those that are vision loss in general into low vision (mod + severe) and blind
	************************************************************************
	if 1==1 {
		// Get proportion of low vision (mod + severe) and blind of all vision loss 
			use `low_mod_envelope', clear
			rename draw_* low_mod_bestcorrected_draw_*
			merge 1:1 age_group_id using `low_sev_envelope', nogen
			rename draw_* low_sev_bestcorrected_draw_*
			merge 1:1 age_group_id using `blind_envelope', nogen
			rename draw_* blind_bestcorrected_draw_*
	
			quietly {
				forvalues c=0/999 {
					gen total = low_mod_bestcorrected_draw_`c' + low_sev_bestcorrected_draw_`c' + blind_bestcorrected_draw_`c'
					
					gen low_bestcorrected_draw_`c' = low_mod_bestcorrected_draw_`c' + low_sev_bestcorrected_draw_`c'
					
					foreach sev in low blind {		
						gen `sev'_prop_draw`c'= `sev'_bestcorrected_draw_`c'/total
					}
					
					drop low_mod_bestcorrected_draw_`c' low_sev_bestcorrected_draw_`c' blind_bestcorrected_draw_`c' low_bestcorrected_draw_`c' total					
				}
			}
			
			tempfile envelope_proportions
			save `envelope_proportions', replace
			
			// apply proprotions for vita meng_pneumo meng_hib meng_meningo meng_other enceph
				foreach name in vita meng_pneumo meng_hib meng_meningo meng_other enceph {

					
					use ``name'_temp', clear
					
					//Vitamin A deficiency does not occur under age 0.1
					foreach num of numlist 0/999 {
						qui if "`name'" == "vita" replace draw_`num' = 0 if inlist(age_group_id, 2, 3, 164) 
					}

					//Load crosswalk map for meningitis from <6/12 to <6/18
					if regexm("`name'", "meng") | "`name'" == "enceph" merge 1:1 age_group_id using `mild_mod_xwalk', nogen

					merge 1:1 age_group_id using `envelope_proportions', nogen
					
					forvalues c=0/999 {
						//Crosswalk meningitis <6/12 to <6/18
						cap replace draw_`c' = draw_`c' * mild_mod_draw_`c'
						cap drop mild_mod_draw_`c'

						gen low_`name'_draw_`c' = low_prop_draw`c' * draw_`c'
						gen blind_`name'_draw_`c' = blind_prop_draw`c' * draw_`c'
						drop draw_`c' low_prop_draw`c' blind_prop_draw`c'
					}
					
					preserve
						drop blind_`name'_draw_*
						rename low_`name'_draw_* draw_*
						replace variable_name = "low_`name'"
						tempfile low_`name'_temp
						save `low_`name'_temp', replace

					restore

						drop low_`name'_draw_* 
						rename blind_`name'_draw_* draw_*
						replace variable_name = "blind_`name'"
						tempfile blind_`name'_temp
						save `blind_`name'_temp', replace
					} // next etiology 
		}			

	************************************************************************
		** 	2. proportionally split low vision (mod + sev) into low vision mod and low vision severe
	************************************************************************		
	if 1==1 {
		// Get proportion of mod and sev vision of all low vision (mod + severe)
			use `low_mod_envelope', clear
			rename draw_* low_mod_bestcorrected_draw_*
			merge 1:1 age_group_id using `low_sev_envelope', nogen
			rename draw_* low_sev_bestcorrected_draw_*

			quietly {
				forvalues c=0/999 {
					gen total = low_mod_bestcorrected_draw_`c' + low_sev_bestcorrected_draw_`c'
					
					foreach sev in low_mod low_sev {		
						gen `sev'_prop_draw`c'= `sev'_bestcorrected_draw_`c'/total
					}
					
					drop total low_mod_bestcorrected_draw_`c' low_sev_bestcorrected_draw_`c'					
				}
			}
			
			tempfile envelope_proportions
			save `envelope_proportions', replace
		// apply proprotions for etiologies
		
			foreach name in trach glauc cat mac other vita diabetes meng_pneumo meng_hib meng_meningo meng_other enceph {
				di in red "`name' split low vision"
				use `low_`name'_temp', clear
				
				merge 1:1 age_group_id using `envelope_proportions', nogen

				forvalues c=0/999 {
					gen low_mod_`name'_draw_`c' = low_mod_prop_draw`c' * draw_`c'
					gen low_sev_`name'_draw_`c' = low_sev_prop_draw`c' * draw_`c'
					drop draw_`c' low_sev_prop_draw`c' low_mod_prop_draw`c'
				}
				
				preserve
					drop low_mod_`name'_draw_* 
					rename low_sev_`name'_draw_* draw_*
					replace variable_name = "low_sev_`name'"
					tempfile low_sev_`name'_temp
					save `low_sev_`name'_temp', replace	
				
				restore

					drop low_sev_`name'_draw_* 
					rename low_mod_`name'_draw_* draw_*
					replace variable_name = "low_mod_`name'"
					tempfile low_mod_`name'_temp
					save `low_mod_`name'_temp', replace
					
		
			}
	}





************************
** Moderate Low vision/ Severe Low vision/ Blind squeeze
************************
	// get locals
	insheet using "`in_dir'/output_vision_meid_codebook.csv", comma clear
	count
	local r = r(N)
	di "`r' rows idenitified"

	foreach sev in low_mod low_sev blind { 
		levelsof variable_name if sev == "`sev'", local(`sev'_names) c
		}

	* local sev low_mod
	foreach sev in low_mod low_sev blind { 
		// pull in the envelop file
		use ``sev'_envelope', clear

		foreach name of local `sev'_names {
			di "append `sev' `name'"
			append using ``name'_temp'
			}
		
		order variable_name age_group_id draw_* 
		keep variable_name age_group_id draw_* 
		tempfile `sev'_all_data
		save ``sev'_all_data', replace
				
		
		// gen total draws for secondary causes
		preserve
		drop if variable_name == "" 
		drop if variable_name == "`sev'_rop" 
		replace variable_name = ""
		rename draw_* total_*

		collapse (sum) total_*, by(variable_name age_group_id) fast
		tempfile total_`sev'
		save `total_`sev'', replace
		

		clear
		restore, preserve

		
		//Save rop draws, as they will not be squeezed 
		keep if variable_name == "`sev'_rop"
		replace variable_name = ""
		rename draw_* `sev'_rop_*
		tempfile `sev'_rop
		save ``sev'_rop', replace
		clear
		restore, preserve

		
		// get proportion for squeezing
		merge 1:1 variable_name age using `total_`sev'', keep(3) nogen

			//Remove ROP from envelope 
			merge 1:1 age variable_name using ``sev'_rop', keep(3) nogen 


		forvalues c = 0/999 {
		
				levelsof draw_`c' if age == 164, local(birth_env)
				levelsof `sev'_rop_`c' if age == 3, local(rop_birth) 
				if `rop_birth' > 0.95 * `birth_env' replace `sev'_rop_`c' = 0.95 * `birth_env' 
			
			gen envelope_`c' = draw_`c' - `sev'_rop_`c'
			gen squeeze_`c' = envelope_`c' / total_`c'  


		//Resave rop
		save ``sev'_rop', replace

		keep age_group_id squeeze_* envelope_*
		tempfile squeeze_`sev'
		save `squeeze_`sev'', replace

		//Resave rop with just variables of interest 
		use ``sev'_rop', clear 
		keep variable_name age `sev'_rop_*
		save ``sev'_rop', replace
		
		clear

		// squeeze  and save secondary causes
		restore		
		merge m:1 age_group_id using `squeeze_`sev'', keep(1 3) nogen
		merge m:1 age_group_id using ``sev'_rop', keep(1 3) nogen

	
		forvalues c = 0/999 {
			qui replace squeeze_`c' = 1 if variable_name == "" //Don't squeeze envelope
			qui replace draw_`c' = draw_`c' * squeeze_`c' 

		}

		drop squeeze_* envelope_* `sev'_rop_*
		sort variable_name age
		tempfile working_`sev'_temp
		save `working_`sev'_temp', replace
	}

clear

************************
** Apply a post hoc cap on prevalence of vitamin a and ROP 
************************	
	foreach sev in low_mod low_sev blind { 
		use `working_`sev'_temp', clear
		
		forvalues c=0/999 {
			noisily di in red "DRAW `c'! `step_name', vita cap, `sev', loop 2"
			
			summ draw_`c' if variable_name == "`sev'_vita" & age_group_id == 6 
			local vita_cap= `r(mean)'
			
			replace draw_`c'= `vita_cap' if variable_name == "`sev'_vita" & age_group_id > 6 & draw_`c' > `vita_cap' 
		}
	
		preserve
		keep if variable_name == "`sev'_vita"
		replace variable_name = ""
		rename draw_* `sev'_vita_*
		tempfile `sev'_vita
		save ``sev'_vita', replace
		clear
		restore

		
		merge m:1 variable_name age_group_id using ``sev'_vita', keep(1 3) nogen
		merge m:1 variable_name age_group_id using ``sev'_rop', keep(1 3) nogen
	
		forvalues c = 0/999 {
			qui replace draw_`c' = draw_`c' - `sev'_vita_`c' - `sev'_rop_`c' if variable_name == ""
		} 
		drop `sev'_vita_* `sev'_rop_*



		** gen total draws for secondary causes without vita and rop 
		preserve
		drop if variable_name == "" | variable_name == "`sev'_vita" | variable_name == "`sev'_rop"
		replace variable_name = ""
		rename draw_* total_*
		collapse (sum) total_*, by(variable_name age_group_id) fast
		tempfile total
		save `total', replace
		clear
		restore
		
		** get proportion for squeezing
		preserve
		merge m:1 variable_name age_group_id using `total', keep(3) nogen
		
		
		
		forvalues c = 0/999 {
			qui gen squeeze_`c' = draw_`c' / total_`c' 

		}
		
		keep age_group_id squeeze_*
		tempfile squeeze
		save `squeeze', replace
		clear

		** squeeze  and save secondary causes
		restore
		drop if variable_name == ""
	
		merge m:1 age_group_id using `squeeze', keep(1 3) nogen
			
		forvalues c = 0/999 {
			qui replace squeeze_`c' = 1 if variable_name == "`sev'_vita" | variable_name == "`sev'_rop"
			qui replace draw_`c' = draw_`c' * squeeze_`c'

		}
	
		
		drop squeeze_*
		cap drop if variable_name == ""
		sort variable_name age_group_id
		tempfile working_`sev'_temp
		save `working_`sev'_temp', replace
		
	}

**********************
** SAVE the vision modelable entities...			
**********************	
	insheet using FILEPATH, clear
	count
	local r = r(N)
	di "`r' rows idenitified"
	
	preserve
	forvalues row=1/`r' {
		keep in `row'
		levelsof acause, local(acause) c
		levelsof grouping, local(grouping) c
		levelsof healthstate, local(healthstate) c
		levelsof sev, local(sev) c
		levelsof variable_name, local(name) c
		levelsof modelable_entity_id, local(meid) c
	
		// bring in the specific etiology/sev for this modelable entity
		use `working_`sev'_temp', clear
		keep if variable_name == "`name'"
		drop variable_name
		
		// save this one
		di in red "SAVING `name'"
		
		cap mkdir FILEPATH
		outsheet using FILEPATH, comma replace
		
		restore, preserve
	}
	restore, not
								
	
	clear
	
		//Next sex	
		}
	//Next year 
	}
