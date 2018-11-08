/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 2: G6PD
Part B: Birth prevalence of Kernicterus due to G6PD deficiency
		
******************************************************************************/

	clear all
	set graphics off
	set more off
	set maxvar 32000


	/*  //////////////////////////////////////////////
			WORKING DIRECTORY
	////////////////////////////////////////////// */ 

	// discover root 
		if c(os) == "Windows" {
			local j FILEPATH
 
			quietly do FILEPATH
		}
		if c(os) == "Unix" {
			local j FILEPATH
			ssc install estout, replace 
			ssc install metan, replace
		} 
		
	// functions
	quietly do FILEPATH
	adopath + FILEPATH

	// directories 
		local working_dir  FILEPATH

		local in_dir FILEPATH

		local out_dir FILEPATH
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
		
		log

		
	
/* ///////////////////////////////////////////////////////////
// G6PD BIRTH PREVALENCE CALCULATIONS
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

		merge 1:m location_id using `nmr', keep(3) nogen  

 
		keep location_name location_id sex year nmr
		tempfile neo
		save `neo'
	

 
	di "importing data"
	cd "`in_dir'"
	local files: dir . files "*_prev.csv"
	local files: list sort files 
	import delimited using `=word(`"`files'"', wordcount(`"`files'"'))', clear
	
 
		merge 1:1 location_id year sex using `neo', keep(3) nogen
		
 

		di in red "calculating scalar parameters"
		local baseline_ehb_scalars 0.0013 0.00085 0.002
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
		quietly{
			forvalues i=0/999{
				if mod(`i', 100)==0{
							di in red "working on number `i'"
				}
 
				local baseline_ehb_scalar = rbeta(`baseline_ehb_alpha', `baseline_ehb_beta')
				local high_ehb_scalar = rgamma(`high_ehb_k', `high_ehb_theta')
				
 
				gen ehb_draw_`i' = draw_`i' * `baseline_ehb_scalar'
 
				replace ehb_draw_`i' = ehb_draw_`i' * `high_ehb_scalar' if nmr>15
				
				//pull kern scalars
				foreach scalar_type in low mid high{
					local `scalar_type'_kern_scalar = rbeta(``scalar_type'_kern_alpha', ``scalar_type'_kern_beta')
				}
				
 
				gen kernicterus_draw_`i' = ehb_draw_`i' * `low_kern_scalar' if nmr<5
				replace kernicterus_draw_`i' = ehb_draw_`i' * `mid_kern_scalar' if (nmr>=5 & nmr<15)
				replace kernicterus_draw_`i' = ehb_draw_`i' * `high_kern_scalar' if nmr>15
				
			}
		}

 

	foreach disease_type in ehb kernicterus{
 
		preserve
			keep year sex location_id location_name `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save "`out_dir'/g6pd_`disease_type'_all_draws.dta", replace	
			export delimited using "`out_dir'/g6pd_`disease_type'_all_draws.csv", replace
		
 
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_name location_id year sex 
			save "`out_dir'/g6pd_`disease_type'_summary_stats.dta", replace
			export delimited using "`out_dir'/g6pd_`disease_type'_summary_stats.csv", replace
		restore
		
	}
 