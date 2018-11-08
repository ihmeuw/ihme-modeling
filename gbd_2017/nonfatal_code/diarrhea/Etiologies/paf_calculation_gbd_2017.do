// Purpose: Calculate diarrhea PAFs including DisMod output, misclassification correction, 
// and odds ratios from GEMS and MALED
// This particular version of this file is for case definition of qPCR Ct value below lowest inversion in accuracy.
// It also uses the fixed effects only from the mixed effects logistic regression models.

clear all
set more off
set maxvar 32000

// Set directories //
if c(os) == "Windows" {
	global j "J:"
	
	set mem 1g
}
if c(os) == "Unix" {
	global j "/home/j"
}

// Read central functions //
qui do "FILEPATH/get_draws.ado"			
qui do "FILEPATH/get_best_model_versions.ado"	

// Sensitivity/specificity matrix using GEMS and MALED //
use "FILEPATH/adjustment_matrix_bimodal.dta", clear
tempfile matrix
save `matrix'

// This one has GEMS odds ratios from this year (GLMER) //
import delimited "FILEPATH/full_odds_results_maled-list2_gems_both.csv", clear

tempfile odds
save `odds'
keep if study=="GEMS"
tempfile odds_gems
save `odds_gems'

use `odds', clear
keep if study=="MALED"
tempfile odds_maled
save `odds_maled'

/// GEMS Etiologies ///
import delimited "FILEPATH/eti_rr_me_ids.csv", clear
keep if source == "GEMS"
levelsof modelable_entity_id, local(ids)

// Vectors for loop //
local year 1990 1995 2000 2005 2010 2017
local sexes 1 2

// Great! Start loop where each time this file is run, it is a separate location but single modelable_entity //
// The local 1 is imported in a file that launches this code script separately for each location_id //
foreach 4 in `ids'{ 
	use "FILEPATH/dismod_eti_covariates.dta", clear
		keep if modelable_entity_id==`4'
		tempfile scalar
		save `scalar'

// Pull the proportion draws //
		get_draws, gbd_id_type(modelable_entity_id) source(epi) gbd_id(`4') location_id(`1') year_id(1990 1995 2000 2005 2010 2017) sex_id(1 2) clear
	di "Pulled draws"
				egen median = rowpctile(draw*), p(50)
				gen floor = median * 0.01 // Consistent with DisMod default floor
			tempfile draws
			save `draws'
			local best = model_version_id[1]
	di "Saved draws"
	/// This is pulling in the coefficient for hospitalized cases from the 'best' DisMod model ////
		use `scalar', clear
						
		keep scalar* model_version_id modelable_entity_id
		merge 1:m modelable_entity_id using `draws', force

		drop _m
		keep if age_group_id!=22

	/// This is a pre-created matrix of sensitivity and specificity draws comparing lab to qPCR case definition ///
		merge m:1 modelable_entity_id using `matrix', keep(3) nogen
			cap drop _m cause_id
		tempfile full
		save `full'
	
	//// Calculate YLDs using odds from MAL-ED ////

		merge m:1 age_group_id modelable_entity_id using `odds_maled', keep(3) nogen force
		cap drop _m dup
		sort age_group_id	
	di "Merged odds"
		qui	forval i = 1/1000{
			local j = `i'-1
			gen proportion_`j' = (draw_`j'+ specificity_`i' - 1)/(sensitivity_`i' + specificity_`i' - 1)
			replace proportion_`j' = 1 if proportion_`j' > 1
		// Consistent with DisMod default //
		replace proportion_`j' = floor if proportion_`j' < 0
			gen paf_`j' = proportion_`j' * (1-1/odds_`i')
			replace paf_`j' = 1 if paf_`j' >1
			replace paf_`j' = 0.000001 if paf_`j' < 0
		}
		drop scalar_* sensitivity_* specificity_* draw_* odds_* proportion_*

		local rei = rei[1]
		keep age_group_id cause_id rei_id year_id sex_id rei_name location_id paf_*

		cap mkdir "FILEPATH/`rei'/"
		
		sort year_id age_group_id sex_id
		export delimited "FILEPATH/paf_yld_`1'.csv", replace
	di "Saved YLDs"	
	
//// Calculate YLLs using odds from GEMS ////
		use `full', clear
		merge m:1 age_group_id modelable_entity_id using `odds_gems', keep(3) nogen force
		cap drop _m dup
		sort age_group_id	

		qui forval i = 1/1000{
			local j= `i' - 1
			replace draw_`j' = draw_`j' * scalar_`j'
			gen proportion_`j' = (draw_`j'+ specificity_`i' - 1)/(sensitivity_`i' + specificity_`i' - 1)
			replace proportion_`j' = 1 if proportion_`j' > 1
		// Consistent with DisMod default //
		replace proportion_`j' = floor if proportion_`j' < 0
			gen paf_`j' = proportion_`j' * (1-1/odds_`i')
			replace paf_`j' = 1 if paf_`j' >1
			replace paf_`j' = 0.000001 if paf_`j' < 0
		}

		local rei = rei[1]
		keep age_group_id cause_id rei_id year_id sex_id rei_name location_id paf_* 

		sort year_id age_group_id sex_id
		export delimited "FILEPATH/paf_yll_`1'.csv", replace
	di "Saved YLLs"
}
di "Finished!!!"
// END //
