/// This pulls the age pattern for cholera at the global level. //
// The model_version_id should be static (not the CFR model)
// do "/filepath/00_age_pattern_draws.do"

clear all
set more off
	
qui do "/filepath/get_draws.ado"

get_draws, source(epi) gbd_id_type(modelable_entity_id) gbd_id(1182) version_id(202961) year_id(2017) decomp_step("step1") location_id(1) clear
save "/filepath/age_pattern_draws.dta", replace
/*
expand 2 if age_group_id==32
replace age_group_id=235 if _n>=45
*/
