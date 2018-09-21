/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER      
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
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
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
	local local_tmp_dir "FILEPATHp"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

	*directory for standard code files
	adopath + FILEPATH

	** Set locals from arguments passed on to job
	local locyear "`1'"
	local restrict "`2'"
	local model_parent "`3'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/sequelae_`locyear'_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir

/*====================================================================
                        1: RESTRICTED LOCYEARS
====================================================================*/


*--------------------1.1: Save zeroes file for every sequelae meid/metric/sex

		if `restrict'==1 {
		
			di in red "`Location-Year is Restricted"
			
			local metrics 5 6
			local meids 1485 1486 2796
			local sexes 1 2
			
			foreach meid in `meids'{
			foreach metric in `metrics'{
			foreach sex in `sexes'{
			
					use `tmp_dir'/zeroes/zeroes_`locyear'.dta, clear
					keep if sex_id==`sex'

					gen measure_id=`metric'
					gen modelable_entity_id=`meid'
						
					keep measure_id location_id year_id age_group_id sex_id draw* modelable_entity_id
					order measure_id location_id year_id age_group_id sex_id draw* modelable_entity_id
					
					levelsof location_id,local(location) clean
					levelsof year_id,local(year) clean
					levelsof sex_id,local(sex) clean
				
					quietly outsheet using "`out_dir'/`meid'_prev/`metric'_`location'_`year'_`sex'.csv", comma replace
					
					sleep 200
			}
			}
			}

		}


/*====================================================================
                        2: ENDEMIC LOCYEARS
====================================================================*/


		else if `restrict'==0 {

*--------------------2.1:Create draws of proportions of abdominal, respiratory and epileptic echino	
		*Create thousand draws of proportions for abdominal, respiratory and epileptic symptoms among echinococcosis cases that
		*add up to 1, given the observed sample sizes in Eckert & Deplazes, Clinical Microbiology Reviews 2004; 17(1) 107-135 (Table 3).
		*Assume that the observed cases follow a multinomial distribution cat(p1,p2,p3), where (p1,p2,p3)~Dirichlet(a1,a2,a3),
		*where the size parameters of the Dirichlet distribution are the number of observations in each category (must be non-zero).

		local n1 = 316+17+15+9+1
			*Abdominal or pelvic cyst localization
		local n2 = 79+5
			*thoracic cyst localization (lungs & mediastinum)
		local n3 = 4
			*brain cyst localization
		local n4 = 10+3
			*other localization (bones, muscles, and skin)

		forvalues i = 0/999 {
			quietly clear
			quietly set obs 1

			generate double a1 = rgamma(`n1', 1)
			generate double a2 = rgamma(`n2', 1)
			generate double a3 = rgamma(`n3', 1)
			generate double a4 = rgamma(`n4', 1)
			generate double A = a1 + a2 + a3 + a4

			generate double p1 = a1 / A
			generate double p2 = a2 / A
			generate double p3 = a3 / A
			generate double p4 = a4 / A

			local p_1485_`i' = p1 + p4
			local p_1486_`i' = p2
			local p_2796_`i' = p3

			di "`p_1485_`i''  `p_1486_`i''  `p_2796_`i''"
		}

*--------------------2.2: Bring in draws from parent model to be split & get macrolist info

		*Draws file	
			use `tmp_dir'/parent_draws/draws1484_`locyear'.dta, clear
			
		*Define needed locals
			levelsof location_id,local(location)
			levelsof year_id,local(year)
			
			local measures 5 6
			local meids 1485 1486 2796
			local sexes 1 2

		* Loop over years/sexes/metrics to multiply echino incidence/prevalence among with the proportions of sequelae  
			
			foreach sex in `sexes' {
			foreach measure in `measures' {
				
				local monitornum 0
			
			foreach meid in `meids' {	
		
					noisily display in red "`meid' `measure' `location' `year' `sex'"
					quietly {
						
						*for this sex/metric/meid
							preserve
							keep if sex_id==`sex' & measure_id==`measure'
						
						*multiply the draw by the proportion calculated above
							forvalues i = 0/999 {
								quietly replace draw_`i' = draw_`i' * `p_`meid'_`i''
							}

						*outsheet excel file of newly adjusted draws
							
							keep measure_id location_id year_id age_group_id sex_id draw* modelable_entity_id
							order measure_id location_id year_id age_group_id sex_id draw* modelable_entity_id
							
							quietly outsheet using "`out_dir'/`meid'_prev/`measure'_`location'_`year'_`sex'.csv", comma replace
							noisily di in red "SAVED!"
							

						*restore and drop that data from the drawfile to go faster in the next round once all 3 meids are done
							restore
					}
					
					local ++monitornum
					if `monitornum'==3 {
						drop if sex_id==`sex' & measure_id==`measure'
					}
							
							
				}
				}
				
				di "COMPLETE: `meid'"
				
				}
				
				di in red "SUCCESS!!"
				
}	




***************************
file open progress using `progress_dir'/`locyear'.txt, text write replace
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


