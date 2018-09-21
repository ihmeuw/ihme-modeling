//// Pull and convert Clostridium DisMod model, create and save PAFs by draw ////
// IHME internal filepaths have been replaced with FILEPATH //
// Thanks for reading! //

clear all
set more off
set maxvar 32000

qui do "FILEPATH/get_draws.ado"	
qui do "FILEPATH/get_population.ado"	

/// Clostridium ///
// Get age groups //
import delimited "FILEPATH/age_mapping.csv", clear
keep if rei == 1
levelsof age_group_id, local(ages)

local year 1990 1995 2000 2005 2010 2016
local sex 1 2

qui foreach y in `year' {
	foreach 3 in `sex' {
		get_population, location_id(`1') year_id(`y') sex_id(`3') age_group_id(`ages') clear
		tempfile pop
		save `pop'

	levelsof year_id, local(yr)
	
	get_draws, source(codcorrect) gbd_id_field(cause_id) gbd_id(302) age_group_ids(`ages') sex_ids(`3') year_ids(`yr') location_ids(`1') measure_ids(1) status(latest) clear
		replace year_id = 2005 if year_id == 2006
	
	/// CoDCorrect draws are number of deaths so make sure to divide by population! ///
			merge 1:1 age_group_id using `pop', nogen
			forval j = 1/1000{
				local i = `j'-1
				rename draw_`i' mort_`i'
				replace mort_`i' = mort_`i'/population
			}
			drop population
			tempfile mortality
			save `mortality'

	// Incidence and Prevalence //
		get_draws, gbd_id_field(modelable_entity_id) source(dismod) gbd_id(1181) age_group_ids(`ages') location_ids(`1') year_ids(`y') sex_ids(`3') measure_ids(6) clear
		forval j = 1/1000 {
			local i = `j'-1
			rename draw_`i' incidence_`j'
		}
		tempfile morbidity
		save `morbidity'

		// mtspecific
		get_draws, gbd_id_field(modelable_entity_id) source(dismod) gbd_id(1227) age_group_ids(`ages') location_ids(`1') year_ids(`y') sex_ids(`3') measure_ids(15) clear

		merge m:m location_id year_id sex_id age_group_id using `mortality', keep(3) nogen
			qui forval i = 1/1000 {
				local j = `i'-1
				gen paf_`j' = draw_`j' / mort_`j' 
				replace paf_`j' = 1 if paf_`j' > 1
				replace paf_`j' = 0 if paf_`j' < 0
			}
		gen paf_me = 9334
		keep age_group_id year_id sex_id paf* modelable_entity_id paf_me cause_id
		export delimited "FILEPATH/eti_diarrhea_clostridium/paf_yll_`1'_`y'_`3'.csv", replace

		// incidence	
		get_draws, gbd_id_field(modelable_entity_id) source(dismod) age_group_ids(`ages') gbd_id(1227) location_ids(`1') year_ids(`y') sex_ids(`3') measure_ids(6) clear
		merge 1:1 age_group_id using `pop', nogen
		merge m:m location_id year_id sex_id age_group_id using `morbidity', keep(3) nogen
			qui forval i = 1/1000 {
				local j = `i'-1
				gen paf_`j' = draw_`j' / incidence_`i'
				replace paf_`j' = 1 if paf_`j' > 1
				replace paf_`j' = 0 if paf_`j' < 0
			}
		gen cause_id = 302
		gen paf_me = 9334
		keep age_group_id year_id sex_id paf* modelable_entity_id paf_me cause_id
		export delimited "FILEPATH/eti_diarrhea_clostridium/paf_yld_`1'_`y'_`3'.csv", replace

	}
}

