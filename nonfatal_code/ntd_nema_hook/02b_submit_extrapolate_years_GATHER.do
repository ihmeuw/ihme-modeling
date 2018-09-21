/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   01 GBD2016 GATHER  
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 02b
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draw
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/extrapolate_years_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics
	
	get_demographics, gbd_team("epi") clear
		local gbdages="`r(age_group_ids)'"
		local gbdyears="`r(year_ids)'"
		local gbdsexes="`r(sex_ids)'"


/*====================================================================

====================================================================*/
	
		
*--------------------2.1: Loop to prepare data
		
	*Get list of locs
		use "`in_dir'/02a_draws_with_subnationals.dta", clear
		levelsof location_id,local(sthALLlocs)clean

	*Pull in draw file with zeroes for countries without burden
		use "`local_tmp_dir'/01a_zeroes.dta", clear
		tempfile zeroes
		save `zeroes', replace
		
	*Loop through sex, location_id, and year, keep only the relevant data, and outsheet the .csv of interest: prevalence (measue id 5)	
	foreach cause in ascar trich hook {
		foreach intensity in inf_all inf_med inf_heavy {

			use `all_inf_draws_filled' if helminth_type == "`cause'" & intensity == "`intensity'", clear
			tempfile `cause'_draws
			quietly save ``cause'_draws', replace
			display as error "Start Loop: `cause' `intensity'"

			foreach loc of local sthALLlocs {
				foreach year in 1990 2005 2010 {
						forvalues sex = 1/2 {
								
								use ``cause'_draws', clear
								quietly keep if location_id == `loc'

								display as error "Creating .csv for prevalence of: `cause' `intensity' `loc' `year' `sex'"

								preserve
									quietly keep if sex_id == `sex' & year == `year'
									quietly count
									if `r(N)' > 0 {
										 keep age_group_id draw*
										 display as error "Drawfile is available"
									}
									else {
										use `zeroes', clear
										display as error "Drawfile is unavailable. Will be replaced with zeroes"
									}	

									keep age_group_id draw*

									capture mkdir "`tmp_dir'/`cause'_`intensity'"
									quietly outsheet using "`tmp_dir'/`cause'_`intensity'/5_`loc'_`year'_`sex'.csv", comma replace
									display as error "Save Complete"
							
								restore

						}
					}
			}
		}
	}		
	
		


		foreach cause in hook ascar trich {
			foreach intensity in inf_all inf_heavy inf_med {
				foreach loc of local sthALLlocs {
					foreach sex in 1 2 {

						! qsub -N int_`cause'_`intensity'_`i'_`sex' -pe multi_slot 4 -l mem_free=8 -P proj_custom_models "FILEPATH/submit_interpolateparallel.sh" "`cause' `intensity' `loc' `sex'"	
					
					}
					sleep 500
				}
			}
		}

		
/*====================================================================
                        3: Model 2016 using glm with annual rate of change and MDA covariate
====================================================================*/



		foreach cause in ascar trich hook  {
			foreach intensity in inf_all inf_med inf_heavy {

				local n = 0
				foreach loc of local sthALLlocs {
					display in red "Appending draw 2004&2010 draw files for: `cause' `intensity' `loc'"
					
					foreach sex in 1 2 {
							
							foreach year in 2004 2010 {

								quietly insheet using "`tmp_dir'/`cause'_`intensity'/5_`loc'_`year'_`sex'.csv", clear double
								quietly keep age_group_id draw*

								cap generate location_id = "`loc'"
								cap generate sex_id = "`sex'"
								cap generate year_id = `year'

								local ++n
								tempfile `n'
								quietly save ``n'', replace

							}
					}
				}

				clear
				forvalues i = 1/`n' {
				append using ``i''
				}

				save "`tmp_dir'/`cause'_`intensity'_prevalence_draws_2004_2010.dta", replace

			}
		}

		
*--------------------3.2: Load and append MDA coverage data
		
	**************************
	* INDIA DEWORMING SURVEY *
	**************************
		use "`in_dir'/01d_indiamda.dta",, clear
		tempfile indiamda
		save `indiamda', replace
	
	********************
	* WHO PCT DATABANK *
	********************
		use "`in_dir'/01b_whopctdatabank.dta", clear
		tempfile whopct
		save `whopct', replace
		
	*Append
		append using `indiamda'
		
*--------------------3.2: Calculate Cumulative Treatments

		collapse (sum) natCovPreSAC natCovSAC, by (location_id year)

		preserve
			keep if year >= 2004 & year < 2010
			collapse (sum) natCovPreSAC natCovSAC, by (location_id)
			rename natCovPreSAC cumPreSAC2004
			rename natCovSAC cumSAC2004
			tempfile cum2004
			save `cum2004', replace
		restore
		preserve
			keep if year >= 2010
			collapse (sum) natCovPreSAC natCovSAC, by (location_id)
			rename natCovPreSAC cumPreSAC2010
			rename natCovSAC cumSAC2010
			tempfile cum2010
			save `cum2010', replace
		restore

   
	   use "`local_tmp_dir'/pop_env.dta", clear
	   tempfile pop_env
	   save `pop_env', replace
   

	foreach cause in ascar trich hook {
	foreach intensity in inf_all inf_heavy inf_med {
		  
			if "`intensity'" == "inf_all" {

				di in red "Starting 2016 estimation for: `cause' `intensity'"		
				
				quietly use "`tmp_dir'/`cause'_`intensity'_prevalence_draws_2004_2010.dta", clear
				
				*Format draws for regression
					egen double mean = rowmean(draw_*)
					drop draw_*
					destring location_id, replace
					destring sex_id, replace
					tempfile prevd
					save `prevd', replace
					
					use `pop_env', clear
					keep if inlist(year_id, 2004,2010) & sex !=3 & age_group_id >=4
					keep location_id age_group_id sex_id year_id
					joinby location_id age_group_id sex_id year_id using "`prevd'", unmatched(none)
					capture drop drawmean
					reshape wide mean , i(age_group_id location_id sex_id) j(year_id)

					replace mean2004 = mean2010 if location_id==30
				
					replace mean2004 = mean2010 if location_id==112
					replace mean2004 = mean2010 if location_id==116
					replace mean2004 = mean2010 if location_id==117				
				
					generate double ann_rate = (mean2010/mean2004)^(1/6)

					merge m:1 location_id using `cum2004', keepusing(cumPreSAC2004 cumSAC2004) keep (master match) nogen
						generate cumTreat2004 = cumSAC2004 if age_group_id < 8
						replace cumTreat2004 = cumPreSAC2004 if age_group_id < 6
						replace cumTreat2004 = 0 if age_group_id < 8 & missing(cumTreat2004)
						drop cumPreSAC2004 cumSAC2004
					merge m:1 location_id using `cum2010', keepusing(cumPreSAC2010 cumSAC2010) keep (master match) nogen
						generate cumTreat2010 = cumSAC2010 if age_group_id < 8
						replace cumTreat2010 = cumPreSAC2010 if age_group_id < 6
						replace cumTreat2010 = 0 if age_group_id < 8 & missing(cumTreat2010)
						drop cumPreSAC2010 cumSAC2010
				
					generate treatPerYear2004 = cumTreat2004 / 6
					generate treatPerYear2010 = cumTreat2010 / 6
					meglm ann_rate treatPerYear2004, link(log) || location_id: , startgrid(0.001, 0.01)

					predict rfx if !missing(treatPerYear2010), remeans

					drop treatPerYear2004
					rename treatPerYear2010 treatPerYear2004

					predict ann_rate_pred if !missing(treatPerYear2004), xb fixedonly
					replace ann_rate_pred = exp(ann_rate_pred + rfx)
					  
					replace ann_rate_pred = 1 if ann_rate_pred > 1 & !missing(ann_rate_pred)
				  
					bysort location_id sex_id (age_group_id): egen mean_rate = mean(ann_rate_pred)
					replace ann_rate_pred = mean_rate if missing(ann_rate_pred)
					replace ann_rate_pred = 1 if missing(ann_rate_pred)
					
				*Save
					tempfile `cause'_rate
					save ``cause'_rate', replace			
			}
		
			use "`tmp_dir'/`cause'_`intensity'_prevalence_draws_2004_2010.dta", replace
			destring location_id, replace
			destring sex_id, replace
			keep if year_id == 2010
			drop year_id
		  
			merge 1:1 location_id sex_id age_group_id using ``cause'_rate', keepusing(ann_rate_pred) nogen 

			replace ann_rate_pred = ann_rate_pred^5

			noisily di as error "Adjust draws by ARC"		
			
			forvalues i = 0/999 {
				quietly replace draw_`i' = draw_`i' * ann_rate_pred if ann_rate_pred!=.
			}
		 
		*Save for export
			tempfile `cause'_`intensity'_2016
			save ``cause'_`intensity'_2016', replace

		*Export csv by country and sex for 2016
			foreach loc of local sthALLlocs {
				foreach sex in 1 2 {
						display in red "`cause' `intensity' `loc' `sex' 2016"

						use ``cause'_`intensity'_2016', replace

						quietly keep if location_id == `loc'
						quietly keep if sex == `sex'

						keep age_group_id draw_*

						quietly outsheet using "`tmp_dir'/`cause'_`intensity'/5_`loc'_2016_`sex'.csv", comma replace
						
			
				}
			}

	}
	}


/*====================================================================
                        4: Export and Save
====================================================================*/
		
	
	local nonendemiclocs: list gbdlocs - sthALLlocs

*--------------------4.1: Endemic Locations
	
	foreach cause in ascar trich hook{
	foreach intensity in inf_all inf_heavy inf_med{	
	foreach loc in `sthALLlocs'{
	foreach year in `gbdyears'{
	foreach sex in `gbdsexes'{
	
		quietly{
		
			*Pull in csvs of draws
				quietly insheet using "`tmp_dir'/`cause'_`intensity'/5_`loc'_`year'_`sex'.csv", clear double
						
			*Make sure the have appropriately named columns to merge
				capture gen helminth_type="`cause'"
					replace helminth_type="`cause'"
				capture gen intensity="`intensity'"
					replace intensity="`intensity'"
				capture gen location_id=`loc'
					replace location_id=`loc'
				capture gen year_id=`year'
					replace year_id=`year'
				capture gen sex_id=`sex'
					replace sex_id=`sex'
				
			
			*Generate needed variables for upload
				if helminth_type=="ascar" & intensity=="inf_all"{
					local meid 2999 
				}
				
				if helminth_type=="hook" & intensity=="inf_all"{
					local meid 3000
				}

				if helminth_type=="trich" & intensity=="inf_all"{
					local meid 3001
				}

				if helminth_type=="ascar" & intensity=="inf_heavy"{
					local meid 1513
				}
					
				if helminth_type=="trich" & intensity=="inf_heavy"{
					local meid 1516
				}
					
				if helminth_type=="hook" & intensity=="inf_heavy"{
					local meid 1519
				}

				if helminth_type=="ascar" & intensity=="inf_med"{
					local meid 1514
				}
				
				if helminth_type=="trich" & intensity=="inf_med"{
					local meid 1517
				}
				
				if helminth_type=="hook" & intensity=="inf_med"{
					local meid 1520
				}
			
			*Format
				keep age_group_id draw_* 
			
			*Set lower age groups to zero
				forval x=0/999{
					replace draw_`x'=0 if age_group_id<4
				}			
		
			capture mkdir "`out_dir'/`meid'_prev"
			outsheet using "`out_dir'/`meid'_prev/5_`loc'_`year'_`sex'.csv", comma replace
			
		}
		
		 
	}
	}
	}
	}
	}
	
	
*--------------------4.1: Nonendemic Locations
	
	foreach meid in 2999 3000 3001 1513 1516   1517  1514 {
	foreach loc in `nonendemiclocs'{
	foreach year in `gbdyears'{
	foreach sex in `gbdsexes'{
	
		*Use zeroes file
			use `zeroes', clear
	
		keep age_group_id draw_*
	
		outsheet using "`out_dir'/`meid'_prev/5_`loc'_`year'_`sex'.csv", comma replace
	
	}
	}
	}
	}
	
	

	
/*	
	*Save the results to the database
		run "FILEPATH/save_results.do"
		
		save_results, modelable_entity_id(`meid') mark_best(`mark_best') env(`env') file_pattern(`file_pat') description(`description') in_dir(`in_dir')

		save_results, modelable_entity_id(2999) description("All Ascariaisis: India update mda; subnatestimates non india") in_dir("`out_dir'/2999_prev") metrics(prevalence) mark_best(no) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(3000) description("All Hookworm: 2015 Methods; India update mda; subnatestimates non india") in_dir("`out_dir'/3000_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(3001) description("All Trichuriasis: India update mda; subnatestimates non india") in_dir("`out_dir'/3001_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)
			
		save_results, modelable_entity_id(1513) description("Heavy Ascariasis: India update mda; subnatestimates non india") in_dir("`out_dir'/1513_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(1516) description("Heavy Trichuriasis: India update mda; subnatestimates non india") in_dir("`out_dir'/1516_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(1519) description("Heavy Hookworm: India update mda; subnatestimates non india") in_dir("`out_dir'/1519_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)
			
		save_results, modelable_entity_id(1514) description("Mild Abdomino-Pelvic Ascar: India update mda; subnatestimates non india") in_dir("`out_dir'/1514_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(1517) description("Mild Abdomino-Pelvic Trich: India update mda; subnatestimates non india") in_dir("`out_dir'/1517_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

		save_results, modelable_entity_id(1520) description("Mild Abdomino-Pelvic Hook: India update mda; subnatestimates non india") in_dir("`out_dir'/1520_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

*/
