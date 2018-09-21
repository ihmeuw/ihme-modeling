** set up

adopath + "{FILEPATH}"
run "{FILEPATH}/get_covariate_estimates.ado"
run "{FILEPATH}/get_draws.ado"
run "{FILEPATH}/location_metadata.ado"

get_draws, gbd_id_field(modelable_entity_id) gbd_id({MODELABLE ENTITY ID}) status(best) source(dismod) measure_ids({MEASURE ID}) clear
keep if measure_id=={MEASURE ID}
tempfile cretin
save `cretin', replace

get_location_metadata, location_set_id({LOCATION SET ID}) clear
keep location_id super_region_id super_region_name region_id region_name ihme_loc_id
tempfile region_sr
save `region_sr', replace

get_covariate_estimates, covariate_name_short({COVARIATE NAME}) clear


gen no_cretin=0
replace no_cretin=1 if mean_value >= 0.9
// merge on regions
   merge m:1 location_id using `region_sr', keep(3)nogen
// Replace cretinism as zero in high income countries 
   	replace no_cretin=1 if super_region_id=={HIGH INCOME SUPER REGION ID}
		
// Mark other regions as indicated in the UNICEF SOWC reports that have sufficient household salt iodization
	replace no_cretin=1 if (year_id == 2005 | year_id == 2010 | year_id == 2016) & (region_name == "Central Latin America" | region_name == "Central Europe" | region_name == "Southeast Asia" | region_name == "East Asia")  
	
keep location_id year_id no_cretin
tempfile no_cretin
save `no_cretin', replace	
	
	
// Merge on iodized salt data and set cretinism prev to zero for countries with greater than 90% salt iodization
	use `cretin', clear
	merge m:1 location_id year_id using `no_cretin', keep(3) nogen
	

//Set prevalence to zero for countries where total goiter prevalence is < 20% or national hh consumption of iodized salt is >90%
forvalues x = 0/999 {
		** Convert deaths to death rates
		replace draw_`x' = 0 if no_cretin==1
			}
			
			drop no_cretin

			
replace modelable_entity_id={MODELABLE ENTITY ID}
	replace measure_id={MEASURE ID}
	drop model_version_id 

	levelsof(location_id), local(ids) clean
	levelsof(year_id), local(years) clean

global sex_id "{SEX IDS}"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "{FILEPATH}/`measure_id'_`location_id'_`year_id'_`sex_id'.csv", comma replace
				}
			}
		}
		
	
	// save results and upload
	
	do {FILEPATH}/save_results.do
    save_results, modelable_entity_id({FILEPATH}) description(intellectual disability due to iodine deficiency) mark_best(yes) in_dir({DIRECTORY}) metrics(prevalence)

	

			
			
