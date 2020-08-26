
 /* **************************************************************************
Birth Prevalence and Kernicterus Calculation
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
	}
	if c(os) == "Unix" {
		local j "FILEPATH/j"
		ssc install estout, replace 
		ssc install metan, replace
	} 
	
// functions
adopath + "FILEPATH/stata_functions/"

// directories 	
	local working_dir = "FILEPATH"
	local incom_pregs_dir = "`working_dir'/rhogam_adjusted_pregnancies_all_draws.dta"
	local notfirst_prev_dir = "`working_dir'/notfirst_birth_prev_all_draws.dta"
	local out_dir "`working_dir'/01_D_final_birthprev"
	capture mkdir "`out_dir'"

/* ///////////////////////////////////////////////////////////
// RH DISEASE BIRTH PREVALENCE CALCULATIONS, PART IV: FINAL CALCS
// Multiply Rhogam-adjusted births from part B by the porportion 
// of children who aren't firstborn from part C, then multiply 
// by .15 for a final result.
// Also: Multiply by 0.0725 (0.038, 0.112) to get modsev impairment prop
///////////////////////////////////////////////////////////// */

//1. Rhogam- Adjusted Pregnancies - has rows for male, female and both
	di in red "importing rhogam-adjusted incompatible pregnancy data"
	use "`incom_pregs_dir'", clear
	rename draw* incompatible_draw*

//2. Merge on notfirst birth order prevalence - not split by sex.
	di in red "importing not-firstborn birth prevalence"
	merge m:1 location_id year using "`notfirst_prev_dir'", keep(3) nogen 
	rename draw* birth_order_draw*

//3. Define scalars
// we need to multiply these draws by two scalars(with uncertainty):
// the proportion of rh isoimmunized who will develop EHB: 
// 0.15 ( 0.133, 0.191)
// and the proportion of EHB babies who will go on to develop kernicterus:
// 0.0725 (0.038, 0.112)
// To get proper uncertainty bounds for both of these, we define the parameters
// of a beta distribution for them, and draw from that beta distribution to get 
// the scalar of use when looping through draws.

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

// 4. Run calculations:
// 		Multiply adjusted pregnancies by birth order proportions, then by scalars
	
	forvalues i=0/999{

		if mod(`i', 100)==0{
							di in red "working on number `i'"
						}
	
		//find scalars
		local ehb_scalar = rbeta(`ehb_alpha', `ehb_beta')
		local kern_scalar = rbeta(`kern_alpha', `kern_beta')

		//multiply by scalars, divide by births to get ehb and kernicterus prevalence  
		gen ehb_draw_`i' = (incompatible_draw_`i' * birth_order_draw_`i' * `ehb_scalar') / births
		gen kernicterus_draw_`i' =  ehb_draw_`i' * `kern_scalar'
		drop incompatible_draw_`i' birth_order_draw_`i'
		
	}
	
//5. Save ehb and kern, get summary stats

	foreach disease_type in ehb kernicterus{
		//all draws
		preserve
			keep ihme_loc_id year sex location_id `disease_type'_draw*
			rename `disease_type'_draw* draw*
			save "`out_dir'/rh_disease_`disease_type'_all_draws.dta", replace	
			export delimited using "`out_dir'/rh_disease_`disease_type'_all_draws.csv", replace
		
		//summary stats	
			
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort location_id year sex 
			save "`out_dir'/rh_disease_`disease_type'_summary_stats.dta", replace
			export delimited using "`out_dir'/rh_disease_`disease_type'_summary_stats.csv", replace
		restore
		
	}
