/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 4: Other
 		
******************************************************************************/

	clear all
	set graphics off
	set more off
	set maxvar 32000


	/*  //////////////////////////////////////////////
			WORKING DIRECTORY
	////////////////////////////////////////////// */ 
 
		if c(os) == "Windows" {
			local j FILEPATH
 
			quietly do FILEPATH
		}
		if c(os) == "Unix" {
			local j FILEPATH
			ssc install estout, replace 
			ssc install metan, replace
		} 
		
 
		adopath + FILEPATH
		run FILEPATH

		local working_dir  FILEPATH
		local in_dir FILEPATH
		local out_dir FILEPATH
		local plot_dir FILEPATH 
		
 
		local rh_disease_dir FILEPATH
		local g6pd_dir FILEPATH
		local preterm_dir FILEPATH
		
 
		local c_date = c(current_date)
		local c_time = c(current_time)
		local c_time_date = "`c_date'"+"_" +"`c_time'"
		display "`c_time_date'"
		local time_string = subinstr("`c_time_date'", ":", "_", .)
		local timestamp = subinstr("`time_string'", " ", "_", .)
		display "`timestamp'"
		
 
	
/* ///////////////////////////////////////////////////////////
// OTHER BIRTH PREVALENCE CALCULATIONS
///////////////////////////////////////////////////////////// */

 
		use FILEPATH, clear
		keep location_id sex year q_nn_med
		keep if year>=1980
		rename q_nn_med nmr

		
		replace nmr = nmr*1000
		replace sex = "1" if sex == "male"
		replace sex = "2" if sex == "female" 
		replace sex = "3" if sex == "both"
		destring sex, replace
		drop if sex==3
 
		replace year = year - 0.5
		tempfile nmr
		save `nmr'

 
		get_location_metadata, location_set_id(9) gbd_round_id(5) clear 
		tempfile locations
		save `locations'		
 

		merge 1:m location_id using `nmr', keep(3)
		drop _merge 
 
		keep location_name location_id sex year nmr
		tempfile neo
		save `neo'

 
		di in red "importing rh disease data"
		use "`rh_disease_dir'", clear 
		drop if sex==3
		keep if year==1990 | year==1995 | year==2000 | year==2005 | year==2010 | year==2017
		rename draw_* rh_disease_draw_*
		
		di in red "merging on g6pd data"
 
		merge 1:1 location_id year sex using "`g6pd_dir'", nogen
		rename draw_* g6pd_draw_*
		
		di in red "merging on preterm data"
		merge 1:1 location_id year sex using "`preterm_dir'", keep(3) nogen
		rename draw_* preterm_draw_* 
 
		merge 1:1 location_id year sex using `neo', keep(3)
	
	 
		di in red "calculating scalar parameters"
		local baseline_ehb_scalars 0.00038 0.00033 0.00163
		local high_ehb_scalars 2.45 1.44 4.16
		local low_kern_scalars 0.23 0.099 0.361
		local mid_kern_scalars 0.35 0.12 0.58
		local high_kern_scalars 0.438 0.255 0.621

		foreach scalar_type in baseline_ehb high_ehb low_kern mid_kern high_kern{
			di in red "finding scalar parameters for `scalar_type'"
			local scalar_mean : word 1 of ``scalar_type'_scalars'
			local scalar_lower : word 2 of ``scalar_type'_scalars'
			local scalar_upper : word 3 of ``scalar_type'_scalars'
			local scalar_se = (`scalar_upper' - `scalar_lower')/(2*1.96)
			if `scalar_mean'<1{
				di in red "generating beta parameters"
				local count_n = (`scalar_mean' * (1-`scalar_mean') ) / `scalar_se'^2
				local `scalar_type'_alpha = `count_n' * `scalar_mean'
				local `scalar_type'_beta = `count_n' * (1-`scalar_mean')
				di in red "for `scalar_type', scalar_se is `scalar_se', count is `count_n', alpha is ``scalar_type'_alpha', beta is ``scalar_type'_beta'"
			}
			else{
				di in red "generating gamma parameters"
				local `scalar_type'_k = (`scalar_mean'/`scalar_se') ^2
				local `scalar_type'_theta = (`scalar_se'^2) / `scalar_mean'
				di in red "for `scalar_type', scalar_se is `scalar_se', k is ``scalar_type'_k', theta is ``scalar_type'_theta'"
			}
		}
	
 
		di in red "calculating kernicterus prevalence"
 
			forvalues i=0/999{
				if mod(`i', 100)==0{
							di in red "working on number `i'"
				}
				
				di "generate 'other' prevalence"
				gen other_draw_`i' = 1- (rh_disease_draw_`i' + g6pd_draw_`i' + preterm_draw_`i')
				drop rh_disease_draw_`i' g6pd_draw_`i' preterm_draw_`i'
				
				di "pull ehb scalars"
				local baseline_ehb_scalar = rbeta(`baseline_ehb_alpha', `baseline_ehb_beta')
				local high_ehb_scalar = rgamma(`high_ehb_k', `high_ehb_theta')
				
				di "get baseline ehb prevalence"
				gen ehb_draw_`i' = other_draw_`i' * `baseline_ehb_scalar'
				drop other_draw_`i'
				
				di "multiply by higher value if necessary"
				replace ehb_draw_`i' = ehb_draw_`i' * `high_ehb_scalar' if nmr>15
				
				di "pull kern scalars"
				foreach scalar_type in low mid high{
					local `scalar_type'_kern_scalar = rbeta(``scalar_type'_kern_alpha', ``scalar_type'_kern_beta')
				}
				
				di "convert to kernicterus prevalence"
				gen kernicterus_draw_`i' = ehb_draw_`i' * `low_kern_scalar' if nmr<5
				replace kernicterus_draw_`i' = ehb_draw_`i' * `mid_kern_scalar' if (nmr>=5 & nmr<15)
				replace kernicterus_draw_`i' = ehb_draw_`i' * `high_kern_scalar' if nmr>15
				
			}
		 
	foreach disease_type in ehb kernicterus {
		di "all draws"
		preserve
			keep ihme_loc_id year sex location_id `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save "`out_dir'/other_`disease_type'_all_draws.dta", replace	
			export delimited "`out_dir'/other_`disease_type'_all_draws.csv",  replace
		
		di "summary stats"
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_id year sex 
			save "`out_dir'/other_`disease_type'_summary_stats.dta", replace
			export delimited "`out_dir'/other_`disease_type'_summary_stats.csv", replace
		restore
		
	}

 