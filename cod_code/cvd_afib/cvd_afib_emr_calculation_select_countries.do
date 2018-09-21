clear all
set more off


//set OS
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	adopath + "FILEPATH"
	run "FILEPATH/pdfmaker_Acrobat11.do"
		
	local savedir "FILEPATH/cvd_afib"
// locals
	local ages = "11 12 13 14 15 16 17 18 19 20 30 31 32 235"
	
// get estimates of csmr and prev for 2016
	get_location_metadata, location_set_id(8) clear
	keep location_id ihme_loc_id location_name is_estimate level
	tempfile locs
	save `locs', replace

// get current best model
	get_best_model_versions, entity(modelable_entity) ids(1859) clear
	levelsof model_version_id, local(model_dismod)
	
// get prevalence 
	get_model_results, gbd_team(epi) model_version_id(`model_dismod') measure_id(5) year_id(2016) sex_id(1 2) age_group_id(`ages') clear
	tempfile prev
	save `prev', replace

// get deaths	
	get_model_results, gbd_team(cod) gbd_id(500) sex_id(1 2) year_id(2016) age_group_id(`ages') clear
	tempfile death
	save `death', replace

// get age_weights
	create_connection_string, server("ADDRESS") database("DATABASE") user("USERNAME") password("PASSWORD")
	local conn_string = r(conn_string)
	odbc load, exec("SELECT * FROM shared.age_group_weight") `conn_string' clear 
		
	keep if gbd_round_id ==4 & age_group_weight_description =="IHME standard age weight"
	tempfile weights
	save `weights', replace
	
	merge 1:m age_group_id using `death', keep(3) nogen
	
	gen agestd_csmr = mean_death_rate*age_group_weight_value
	fastcollapse agestd_csmr, by(location_id year_id sex_id) type(sum)
	save `death', replace
	
	use `prev', clear
	merge m:1 age_group_id using `weights', keep(3) nogen
	gen agestd_prev = mean*age_group_weight_value
	fastcollapse agestd_prev, by(location_id sex_id year_id) type(sum)
	

merge 1:1 location_id year_id sex_id using `death', keep(3) nogen
merge m:1 location_id using `locs', assert(2 3)keep(3) nogen
gen ratio = agestd_csmr/agestd_prev
keep if level ==3
	
keep location_id location_name ihme_loc_id agestd_csmr agestd_prev ratio sex_id

outsheet using "FILEPATH/custom_deaths_prev_csmr_`model_dismod'.csv", replace comma 	

twoway (scatter agestd_prev agestd_csmr), by(sex_id, title("Prevalence vs CSMR, Age-standardized") note(" ") legend(off))
graph export "FILEPATH/prev_csmr_agestd_`model_dismod'.eps", replace orientation(landscape)  

graph save "FILEPATH/agestd_prev_csmr.gph", replace