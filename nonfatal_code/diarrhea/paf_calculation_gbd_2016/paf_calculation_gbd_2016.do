// Purpose: Calculate diarrhea PAFs including DisMod output, misclassification correction, and odds ratios from GEMS
// This particular version of this file is for case definition of qPCR Ct value below lowest inversion in accuracy.
// It also uses the fixed effects only from the mixed effects logistic regression models.
// This file is designed to be run in parallel on computing cluster to more rapidly produce results. 
// This is different from GBD 2015 because it uses different odds ratios. //
// Internal IHME filepaths have been replaced with FILEPATH //
// Thanks for reading! //

set more off
set maxvar 32000

// Read IHME central functions //
qui do "FILEPATH/get_draws.ado"			
qui do "FILEPATH/get_best_model_versions.ado"	

// Sensitivity/specificity matrix //
use "FILEPATH/adjustment_matrix_bimodal.dta", clear
tempfile matrix
save `matrix'

// Odds ratios //
import delimited "FILEPATH/odds_ratios_gbd_2016.csv", clear
/// Replace crazy values ///
	qui forval i = 1/1000 {
		replace odds_`i' = 1000 if odds_`i' > 1000
	}
	cap drop _m
	tempfile odds
	save `odds'
	
/// GEMS Etiologies ///

import delimited "FILEPATH/eti_rr_me_ids.csv", clear
keep if source == "GEMS"
levelsof modelable_entity_id, local(ids)

// Vectors for loop //

local year 1990 1995 2000 2005 2010 2016
local sexes 1 2

// Great! Start loop where each time this file is run, it is a separate location but single modelable_entity //
qui foreach 4 in `ids'{ 
// Saved the scalars in a .DTA. Cluster issues may have been from trying to read the same CSV thousands of times //
	use "FILEPATH/dismod_eti_covariates.dta", clear
		keep if modelable_entity_id==`4'
		gen std = (upper_effect-mean_effect)/invnormal(0.975)
		
		qui forval i = 1/1000 {
			gen scalar_`i' = exp(rnormal(mean_effect, std))
		}
		
		tempfile scalar
		save `scalar'
		
	foreach y in `year' {
	
		foreach s in `sexes' {
		
	// Pull the proportion draws //
		get_draws, gbd_id_field(modelable_entity_id) source(dismod) gbd_id(`4') location_ids(`1') year_ids(`y') sex_ids(`s') clear
				egen median = rowpctile(draw*), p(50)
				gen floor = median * 0.01 // Consistent with DisMod dUSERt floor
			tempfile draws
			save `draws'
			local best = model_version_id[1]

	/// This is pulling in the coefficient for hospitalized cases from the 'best' DisMod model ////
		use `scalar', clear
						
			keep scalar* model_version_id modelable_entity_id
			merge 1:m modelable_entity_id using `draws', force

			drop _m
			gen agecat = 1
			replace agecat = 2 if age_group_id==5
			replace agecat = 3 if age_group_id>5
			keep if age_group_id!=22

	/// This is a pre-created matrix of sensitivity and specificity draws comparing lab to qPCR case definition ///
			merge m:1 modelable_entity_id using `matrix', keep(3) nogen
			cap drop _m cause_id
			merge 1:1 age_group_id modelable_entity_id using `odds', keep(3) nogen
			cap drop _m dup
			sort age_group_id	

	//// Calculate YLDs ////
		preserve
			qui	forval i = 1/1000{
				local j = `i'-1
				gen proportion_`j' = (draw_`j'+ specificity_`i' - 1)/(sensitivity_`i' + specificity_`i' - 1)
				replace proportion_`j' = 1 if proportion_`j' > 1
			// Consistent with DisMod dUSERt //
				replace proportion_`j' = floor if proportion_`j' < 0
				gen paf_`j' = proportion_`j' * (1-1/odds_`i')
				replace paf_`j' = 1 if paf_`j' >1
			}
			drop scalar_* sensitivity_* specificity_* draw_* odds_* proportion_*

			local rei = rei[1]
			keep age_group_id cause_id rei_id sex_id rei_name location_id paf_*
			bysort age_group_id: gen num = _n
			drop if num > 1
			drop num
		cap mkdir "FILEPATH/`rei'/"
		
		export delimited "FILEPATH/`rei'/paf_yld_`1'_`y'_`s'.csv", replace
		
	//// Calculate YLLs ////
		restore
			qui forval i = 1/1000{
				local j= `i' - 1
				replace draw_`j' = draw_`j' * scalar_`i'
				gen proportion_`j' = (draw_`j'+ specificity_`i' - 1)/(sensitivity_`i' + specificity_`i' - 1)
			// Consistent with DisMod dUSERt //
				replace proportion_`j' = floor if proportion_`j' < 0
				gen paf_`j' = proportion_`j' * (1-1/odds_`i')
				replace paf_`j' = 1 if paf_`j' >1
			}
			drop scalar_* sensitivity_* specificity_* draw_* odds_* proportion_*

			local rei = rei[1]
			keep age_group_id cause_id rei_id sex_id rei_name location_id paf_*
			bysort age_group_id: gen num = _n
			drop if num > 1
			drop num
		
		export delimited "FILEPATH/`rei'/paf_yll_`1'_`y'_`s'.csv", replace
		}
	}	
}
// END //
