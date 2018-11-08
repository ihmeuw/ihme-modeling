
/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 1: RH DISEASE
Part D: Final birth prevalence calculations

******************************************************************************/

clear all
set more off
set graphics off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 

// priming the working environment 
	if c(os) == "Windows" {
		local j "J:"
		// Load the PDF appending application
		quietly do FILEPATH
	}
	if c(os) == FILEPATH
		ssc install estout, replace 
		ssc install metan, replace
	} 
	
// functions
adopath + FILEPATH
quietly do FILEPATH

// directories 	
	local working_dir = FILEPATH
 
	local incom_pregs_dir = FILEPATH
	local notfirst_prev_dir = FILEPATH
 
	local out_dir "FILEPATH/01_D_final_birthprev"
	capture mkdir "`out_dir'"
 
	
	local plot_dir "`out_dir'/time_series"
	capture mkdir "`plot_dir'"
	
// Create timestamp for logs
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"


	
/* ///////////////////////////////////////////////////////////
// RH DISEASE BIRTH PREVALENCE CALCULATIONS, PART IV: FINAL CALCS
// Multiply Rhogam-adjusted births from part B by the porportion 
///////////////////////////////////////////////////////////// */

 
	di in red "importing rhogam-adjusted incompatible pregnancy data"
	use "`incom_pregs_dir'", clear
	rename draw* incompatible_draw*

 
	di in red "importing not-firstborn birth prevalence"
	merge m:1 location_id year using "`notfirst_prev_dir'", keep(3) nogen 
	rename draw* birth_order_draw*


	di in red "calculating scalar parameters"
	local ehb_scalars 0.15 0.133 0.191
	local kern_scalars 0.0725 0.038 0.112

	foreach scalar_type in ehb kern{
		di in red "finding scalar parameters for `scalar_type'"
		local scalar_mean : word 1 of ``scalar_type'_scalars'
		local scalar_lower : word 2 of ``scalar_type'_scalars'
		local scalar_upper : word 3 of ``scalar_type'_scalars'
		local scalar_se = (`scalar_upper' - `scalar_lower')/(2*1.96)
		local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2
		local `scalar_type'_alpha = `count_n' * `scalar_mean'
		local `scalar_type'_beta = `count_n' * (1-`scalar_mean')
		di in red "for `scalar_type', scalar_se is `scalar_se', count is `count_n', alpha is ``scalar_type'_alpha', beta is ``scalar_type'_beta'"
	}

 
	
	forvalues i=0/999{

		if mod(`i', 100)==0{
							di in red "working on number `i'"
						}
	
 
		local ehb_scalar = rbeta(`ehb_alpha', `ehb_beta')
		local kern_scalar = rbeta(`kern_alpha', `kern_beta')
  
		gen ehb_draw_`i' = (incompatible_draw_`i' * birth_order_draw_`i' * `ehb_scalar') / births
		gen kernicterus_draw_`i' =  ehb_draw_`i' * `kern_scalar'
		drop incompatible_draw_`i' birth_order_draw_`i'
		
	}
	 

	foreach disease_type in ehb kernicterus{

		preserve
			keep ihme_loc_id year sex location_id `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save "`out_dir'/rh_disease_`disease_type'_all_draws.dta", replace	
			export delimited using "`out_dir'/rh_disease_`disease_type'_all_draws.csv", replace
		 
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_id year sex 
			save "`out_dir'/rh_disease_`disease_type'_summary_stats.dta", replace
			export delimited using "`out_dir'/rh_disease_`disease_type'_summary_stats.csv", replace
		restore
		
	}
