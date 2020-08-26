/* **************************************************************************
Birth prevalence of Kernicterus due to all causes (rh disease, g6pd, and other)

Taking the sum of rh disease, g6pd, and other kernicterus birth prevalences 
to get kernicterus birth prevalence for all relevant causes

hemo_kern_prev = rh_kern_prev + g6pd_kern_prev + other_kern_prev
		
******************************************************************************/

clear all
set graphics off
set more off
set maxvar 32000

/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 

		//root dir
	if c(os) == "Windows" {
		local j "J:"
	}
	if c(os) == "Unix" {
		local j "FILEPATH/j"
		ssc install estout, replace 
		ssc install metan, replace
	} 
	
// locals
local target_me_id 3962

// functions
adopath + "FILEPATH/stata_functions/"

// directories 
	local working_dir = "FILEPATH"
	local out_dir "FILEPATH/05_final_kernicterus"
	local upload_dir "FILEPATH"
	
// set up input directories: one for each cause (we don't use preterm, but make the local to preserve indexing)
	local rh_disease_dir "`working_dir'/rh_disease_kernicterus_all_draws.dta"
	local g6pd_dir "`working_dir'/g6pd_kernicterus_all_draws.dta"
	local preterm_dir "`working_dir'/preterm_kernicterus_all_draws.dta"
	local other_dir "`working_dir'/other_kernicterus_all_draws.dta"

/* ///////////////////////////////////////////////////////////
// Sum everything together
///////////////////////////////////////////////////////////// */

//start with rh disease
	di in red "importing for rh disease"
	use if sex!=3 using "`rh_disease_dir'", clear
	rename draw_* rh_disease_draw_*
	
	//keep only dismod years
	keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2015 | year==2017 | year==2019
	
//now g6pd
	di in red "merging on g6pd"
	//merge should be perfect
	merge 1:1 location_id year sex using "`g6pd_dir'", keep(3) nogen
	rename draw_* g6pd_draw_*
	
//now other
	di in red "mergin on other"
	//merge should be perfect
	merge 1:1 location_id year sex using "`other_dir'", keep (3) nogen
	rename draw_* other_draw_*

//now sum
di in red "calculating kernicterus prevalence. for all causes"
			forvalues i=0/999{
				if mod(`i', 100)==0{
							di in red "working on number `i'"
				}
				
				gen draw_`i' = rh_disease_draw_`i' + g6pd_draw_`i' + other_draw_`i'
				drop rh_disease_draw_`i' g6pd_draw_`i' other_draw_`i'
			}  

 ///////////////////////////////////////////////////////////
// Save
///////////////////////////////////////////////////////////// 

	di in red "saving results"
	di "all draws"
		save "`out_dir'/final_kernicterus_all_draws.dta", replace
		export delimited "`out_dir'/final_kernicterus_all_draws.csv", replace
	
	di "summary stats"
		egen mean = rowmean(draw_*)
		fastpctile draw*, pct(2.5 97.5) names(lower upper)
		drop draw*
	
		save "`out_dir'/final_kernicterus_summary_stats.dta", replace
		export delimited "`out_dir'/final_kernicterus_summary_stats.csv", replace
