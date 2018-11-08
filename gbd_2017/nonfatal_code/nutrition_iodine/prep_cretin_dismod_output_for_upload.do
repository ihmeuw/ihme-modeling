** set up
clear all
set more off
set mem 1g
if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	
local out_dir `1'
local data_dir `2'
local loc_id `3'
local model_version_id `4'

local acause nutrition_iodine
local measure prevalence
local measure_id 5			
			

adopath + "FILEPATH"
run "FILEPATH/get_covariate_estimates.ado"
run "FILEPATH/get_draws.ado"
run "FILEPATH/get_location_metadata.ado"

get_draws, gbd_id_type(modelable_entity_id) gbd_id(2929) status(best) source(epi) measure_id(5) location_id(`loc_id') year_id(1990 1995 2000 2005 2010 2017) num_workers(3) clear
keep if measure_id==5
tempfile cretin
save `cretin', replace

import delimited using "`data_dir'/location_metadata.csv", clear
tempfile region_sr
save `region_sr', replace

import delimited using "`data_dir'/iod_salt_cov.csv", clear

gen no_cretin=0
replace no_cretin=1 if mean_value >= 0.9
// merge on regions
   merge m:1 location_id using `region_sr', keep(3)nogen
// Replace cretinism as zero in high income countries 
   	replace no_cretin=1 if super_region_id==64
		
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

			
replace modelable_entity_id=18777
	replace measure_id=5
	drop model_version_id 

	levelsof(location_id), local(ids) clean
	levelsof(year_id), local(years) clean

global sex_id "1 2"

foreach location_id of local ids {
		foreach year_id of local years {
			foreach sex_id of global sex_id {
					qui outsheet if location_id==`location_id' & year_id==`year_id' & sex_id==`sex_id' using "`out_dir'/`measure_id'_`location_id'_`year_id'_`sex_id'.csv", comma replace
				}
			}
		}		
