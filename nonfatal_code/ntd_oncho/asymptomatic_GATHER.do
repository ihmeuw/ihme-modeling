/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER     
Output:           Calculate prevalence of asymptomatic onchocerciasis
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 02
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
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

	*directory for standard code files
	adopath + FILEPATH

	** Set locals from arguments passed on to job
	local location "`1'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/asymptomatic_`location'_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	use `tmp_dir'/skeleton_`location'.dta, clear
		tempfile pop
		save `pop', replace

	levelsof(year_id), local(gbdyears) clean
	levelsof(sex_id), local(gbdsexes) clean
	levelsof(age_group_id), local(gbdages) clean


/*====================================================================
                        2: Get Draws of Sequelae
====================================================================*/


*--------------------2.1: Bring in draws from squeezed vision/blindness

	*Prep
		local squeezedmeids 2957 2958 3611
		*2957 moderate vision impairment due to onchocerciasis
		*2958 severe vision impairment due to onchocerciasis
		*3611 blindness due to onchocerciasis super squeezed

	*Loop
		tempfile whole
		local stack 1

		foreach meid in `squeezedmeids'{

			*Get Draws
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(epi) measure_ids(5) location_ids(`location') year_ids(`gbdyears') age_group_ids(`gbdages') sex_ids(`gbdsexes') status(best) clear
		
				*checkpoint
				quietly count
				di "Meid `meid' Obs = `r(N)'"	
			
			*Merge with population
				merge 1:1 location_id year_id age_group_id sex_id using "`pop'", keep(matched) nogen
			
			*Multiply draws by population to get # cases
				forval x=0/999{
					replace draw_`x'=draw_`x'*population
				}
			
				drop population
			
			*Multiply meids that need to be subtracted by (-1) to facilitate easy subtraction via collapse later
				forval x=0/999{
					replace draw_`x'=draw_`x'*-1
				}
			
			*Scale draws per gbd2015 methods
				forval x=0/999{
					replace draw_`x'=draw_`x'*8/33
				}
			
			*Save/append ready file
				if `stack'>1 append using `whole'
				save `whole', replace

				local ++stack
				pause 1
		}

*--------------------2.2: Bring in draws from sqeezed skin disease
	
	*Prep
		local unsqueezedmeids 1495 2620 1496 2515 2621
		*1495 mild skin disease
		*2620 mild skin disease without itch
		*1496 moderate skin disease
		*2515 severe skin disease
		*2621 severe skin disease without itch

	*Loop
		foreach meid in `unsqueezedmeids'{
				
			*Load file
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(epi) measure_ids(5) location_ids(`location') year_ids(`gbdyears') age_group_ids(`gbdages') sex_ids(`gbdsexes') status(best) clear
		
					*checkpoint
					quietly count
					di "Meid `meid' Obs = `r(N)'"		
				
			*Merge with population
				merge 1:1 location_id year_id age_group_id sex_id using "`pop'", keep(matched) nogen
				
			*Multiply draws by population to get # cases
				forval x=0/999{
					replace draw_`x'=draw_`x'*population
				}
				
				drop population
				
			*Multiply meids that need to be subtracted by (-1) to facilitate easy subtraction via collapse later
				forval x=0/999{
					replace draw_`x'=draw_`x'*-1
				}
						
			*Save/append ready file	
				if `stack'>1 append using `whole'
				save `whole', replace
				
				local ++stack
				sleep 30000
			}

	
*--------------------2.3: Bring in draws from parent

	*Prep
		local parent 1494
		*1494 onchocerciasis

	*Loop
		foreach meid in `parent'{

			*Load file
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(epi) measure_ids(5) location_ids(`location') year_ids(`gbdyears') age_group_ids(`gbdages') sex_ids(`gbdsexes') status(best) clear
			
					*checkpoint
					quietly count
					di "Meid `meid' Obs = `r(N)'"

			
			*Merge with population
				merge 1:1 location_id year_id age_group_id sex_id using "`pop'", keep(matched) nogen
			
			*Multiply draws by population to get # cases
				forval x=0/999{
					replace draw_`x'=draw_`x'*population
				}
				
				drop population
				
			*Save/append ready file
						
				if `stack'>1 append using `whole'
				save `whole', replace
				
				local ++stack
				sleep 30000
		}

		
/*====================================================================
                        3: Calculate Asymptomatic Prevalence
====================================================================*/


*--------------------3.1: Calculate Asx

	use `whole', clear
	
	*Collapse across meids to subtract all from the parent
		run "FILEPATH/fastcollapse.ado"
		fastcollapse draw_*, by(year_id age_group_id sex_id) type(sum)
			
	*Set Minimum to Zero
		forval x=0/999{
			replace draw_`x'=0 if draw_`x'<0
		}
	
	*Rejoin with population and re-calculate prevalence
		merge 1:1 location_id year_id age_group_id sex_id using `pop', keep(matched) nogen
		
		forval x=0/999{
			replace draw_`x'=draw_`x'/population
		}
	


/*====================================================================
                        4: Export File
====================================================================*/


*--------------------4.1: Format for Export
	
	*Format for save_results
		gen location_id=`location'
		gen modelable_entity_id=`meidout'
		gen measure_id=5
		drop population

	*Set output MEID = 3107 (asymptomatic oncho)
		local meidout 3107

*--------------------4.2: Export
	
	outsheet using "`out_dir'/`meidout'_prev/`location'.csv", comma replace




***************************

file open progress using `progress_dir'/`location'.txt, text write replace
file write progress "complete"
file close progress





log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


