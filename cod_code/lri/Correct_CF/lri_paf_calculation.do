// Purpose: Calculate LRI PAFs including DisMod output, static odds ratios, and CFR correction

set more off
set maxvar 32000

qui do "FILEPATH/get_draws.ado"	
use "FILEPATH/dismod_eti_covariates.dta", clear
	tempfile cvs
	save `cvs'
	
// influenza and RSV me_ids //
local id 1259 1269
local sex 1 2
local year 1990 1995 2000 2005 2010 2016

// Loop influenza and RSV by location, year, and sex 
// Creates a pair of PAF csvs for each modelable_entity_id 
// One for YLLs and one for YLDs that will be inputs for the DALYnator //
qui foreach me_id in `id' {
  foreach s in `sex' {
    foreach 2 in `year' { 
// Get DisMod proportion draws //
	get_draws, gbd_id_field(modelable_entity_id) source(dismod) gbd_id(`me_id') location_ids(`1') year_ids(`2') sex_ids(`s') clear
		keep if age_group_id != 22
		drop if age_group_id == 21 | age_group_id == 27 | age_group_id == 164 | age_group_id == 33
		tempfile draws
		save `draws'
		
// DisMod models have a covariate for 'severity' 
// the assumption is that the proportion of LRI cases that are positive for viral etiologies
// is lower among severe than non-severe LRI cases //	
	local best = model_version_id[1]
	
	use `cvs', clear
		keep if modelable_entity_id == `me_id'
		gen std = (upper_effect-mean_effect)/invnormal(0.975)
		
// Create 1000 draws of this severity scalar, is in log-space //
	// do "$j/Project/Causes of Death/CoDMod/Models/B/codes/small codes/gen matrix of draws.do" mean_effect se effect
	//	svmat effect, names(scalar_)
		qui forval i = 1/1000 {
			local j = `i' - 1
			gen scalar_`j' = exp(rnormal(mean_effect, std))
					
		}
		keep scalar_* model_version_id
		merge 1:m model_version_id using `draws', force
		drop _m
		save `draws', replace
	
// Previously created file with 1000 draws of the odds ratio of LRI given pathogen presence //			
	use "FILEPATH/odds_draws.dta", clear
		keep if modelable_entity_id==`me_id'

	merge 1:1 age_group_id using `draws', nogen
		gen rei_id = 190
		replace rei_id = 187 if `me_id' == 1259
		gen rei_name = "eti_lri_flu"
		replace rei_name = "eti_lri_rsv" if `me_id' == 1269
		local rei = rei_name[1]
	
	preserve
// Generate YLD PAF is proportion * (1-1/odds ratio) //
		qui forval i = 0/999 {
			gen paf_`i' = draw_`i' * (1-1/rr_`i')
			replace paf_`i' = 1 if paf_`i' > 1
	/// No etiology in neo-nates ///
			replace paf_`i' = 0 if age_group_id<=3
		}
		drop rr_*
		drop draw_*
		drop scalar_*
		cap drop modelable_entity_id
		cap mkdir "FILEPATH/`rei'/"
		export delimited "FILEPATH/`rei'/paf_yld_`1'_`2'_`s'.csv", replace

	restore

// Generate YLL PAF is proportion * severity_scalar * cfr_scalar * (1-1/odds ratio) //
// This is a previously created file with 1000 draws of the ratio of case fatality among viral to bacterial
// causes of LRI by age //
		merge 1:1 age_group_id using "FILEPATH/cfr_scalar_draws.dta", nogen
		
		qui forval i = 0/999 {
			gen paf_`i' = draw_`i' * (1-1/rr_`i') * scalarCFR_`i' * scalar_`i'
			replace paf_`i' = 1 if paf_`i' > 1
	/// No etiology in neo-nates ///
			replace paf_`i' = 0 if age_group_id<=3
		}

		drop rr_*
		drop draw_*
		drop scalarCFR_*
		drop scalar_*

		cap drop modelable_entity_id
		export delimited "FILEPATH/`rei'/paf_yll_`1'_`2'_`s'.csv", replace
     }
   }
}

