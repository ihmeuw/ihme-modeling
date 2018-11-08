/*====================================================================
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2017
Output:           Estimate prevalence of sequelae of CE
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/


	version 13.1
	drop _all
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
	local gbd = "gbd2017"
	*local envir (dev or prod)
	local envir = "prod"
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
	adopath + "FILEPATH"

	*get task id from environment
		local job_id : env SGE_TASK_ID
		di "`job_id'"
	*get arguments passed through
		local gbdyears = subinstr("`1'","_"," ",.)
		local gbdages = subinstr("`2'","_"," ",.)
		local gbdsexes = subinstr("`3'","_"," ",.)
		local model_parent = "`4'"
		local ce_all_prevalence_meid = "`5'"
		local gbdround = "`6'"
		local metrics = subinstr("`7'","_"," ",.)

		local metrics = subinstr("`metrics'","_"," ",.)

	*get other arguments task-specific
		capture use "`local_tmp_dir'/ce_sequelae_task_directory.dta", clear
			quietly count
			local check = `r(N)'
			local counter 0
			while `check' == 0 & `counter' <= 10 {
				capture use "`local_tmp_dir'/ce_sequelae_task_directory.dta", clear
				quietly count
				local check = `r(N)'
				local ++counter
			}
		keep if id==`job_id'
		local location = location_id
		local year = year_id
		local restrict = restricted
		di "`restrict'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH.smcl", replace
	***********************

	*print macros in log
	macro dir

/*====================================================================
                        1: RESTRICTED LOCYEARS
====================================================================*/

*--------------------1.1: Save zeroes file for every sequelae meid/metric/sex

		if `restrict'==1 {


				di in red "`Location-Year is Restricted"

				*make skeleton using gbd age and sex locals
				local num_sexes : list sizeof local(gbdsexes)
				local num_ages : list sizeof local(gbdages)
				local table_length = `num_sexes'*`num_ages'


clear
set obs `table_length'
egen age_group_id = fill(`gbdages' `gbdages')
egen sex_id = fill(`gbdsexes' `gbdsexes')


				*add empty draws to make zeroes file
				forval x=0/999{
					display in red ". `x' " _continue
					quietly gen draw_`x'=0
				}

				*add missing columns
				expand 2,gen(measure_id)
					replace measure_id=5 if measure_id==1
					replace measure_id=6 if measure_id==0
				gen year_id = `year'
				gen location_id = `location'

				tempfile zeroes
				save `zeroes', replace


			*output files
			local meids 1485 1486 2796

			foreach meid in `meids'{

					use `zeroes', clear

					gen modelable_entity_id=`meid'

					keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
					order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*

					quietly outsheet using "`out_dir'/`meid'/`location'_`year'.csv", comma replace

					sleep 200

					di in red "meid `meid' = success"
			}

		}


/*====================================================================
                        2: ENDEMIC LOCYEARS
====================================================================*/


	else if `restrict'==0 {

*--------------------2.1:Create draws of proportions of abdominal, respiratory and epileptic echino
		*Create thousand draws of proportions for abdominal, respiratory and epileptic symptoms among echinococcosis cases that

		local n1 = 50
			*Abdominal or pelvic cyst localization
		local n2 = 47
			*thoracic cyst localization (lungs & mediastinum)
		local n3 = 3
			*brain cyst localization

		forvalues i = 0/999 {
			quietly clear
			quietly set obs 1

			generate double a1 = rgamma(`n1', 1)
			generate double a2 = rgamma(`n2', 1)
			generate double a3 = rgamma(`n3', 1)
			generate double A = a1 + a2 + a3

			generate double p1 = a1 / A
			generate double p2 = a2 / A
			generate double p3 = a3 / A

			local p_1485_`i' = p1
			local p_1486_`i' = p2
			local p_2796_`i' = p3

		}

*--------------------2.2: Bring in draws from parent model to be split & get macrolist info


	* Get draws
		run FILEPATH/get_draws.ado
		di "`ce_all_prevalence_meid' `metrics' `gbdages' `location' `year' `gbdsexes'"
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(`ce_all_prevalence_meid') measure_id("5 6") age_group_id(`gbdages') location_id(`location') year_id(`year') sex_id(`gbdsexes') source(ADDRESS) status("best") clear

		keep if year_id == `year'
		describe draw_0
		describe age_group_id

		local meids 1485 1486 2796
		di "`meids'"
		* Loop over years/sexes/metrics to multiply echino incidence/prevalence among with the proportions of sequelae

			*multiply the draw by the proportion calculated above
				foreach meid in `meids' {

					replace modelable_entity_id=`meid'

					*split all ce cases proportionally among sequela
					forvalues i = 0/999 {
						quietly replace draw_`i' = draw_`i' * `p_`meid'_`i''
					}

					*zero-out youngest age groups (28 days)
					forvalues i = 0/999 {
						quietly replace draw_`i' = 0 if age_group_id<=3
					}

					*Outsheet
					keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
					order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw*
					quietly outsheet using "`out_dir'/`meid'/`location'_`year'.csv", comma replace

					di in red "meid `meid' = success"

				}


		}



***************************
file open progress using `progress_dir'/`location'_`year'.txt, text write replace
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
