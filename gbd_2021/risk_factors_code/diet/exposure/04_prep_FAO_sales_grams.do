*****************************************************************************************************************************
**  Description: Preps FBS and sales data and add to nutrient data from the SUA
****************************************************************************************************************************

** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
	// Set to run all selected code without pausing
		set more off
	// Remove previous restores
		capture restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}
		
// Close previous logs
	cap log close
	
// call in arguments being passed
	local version "GBD2021"
	local nutrients_version `version'

	local sales_version "2017_1"

// Store filepaths in macros
	adopath + "$FILEPATH/stata"
	local fao_dir "$FILEPATH/FAOSTAT/"
	local fao_file "FAOSTAT_1961_2013_FOOD_BALANCE_SHEETS_PRELIM_NORM_Y2016M02D24"
	local out_dir "$FILEPATH/"
	local FAO_codebook 	"$FILEPATH/FAO_codebook_gbd2016_updated_5.dta" 
	
local prep_data 0

//get locations 
get_location_metadata, location_set_id(9) clear
keep if is_estimate==1
//keep the maximumed version
qui sum location_set_version_id
keep if location_set_version_id==`r(max)'
rename location_name countryname
drop if location_name == "Niger" & level == 4
drop if location_name == "Punjab" & level == 4
drop if inlist(countryname, "Distrito Federal", "North Africa and Middle East", "South Asia")
drop if location_id == 533
tempfile locations
save `locations', replace

if `prep_data' == 1{
	import delimited "`fao_dir'`fao_file'.csv", clear

	keep if unit == "Kg" | unit == "kcal/capita/day"
	
// Keep only FAO itemcodes that we've mapped to GBD categories
	merge m:m itemcode using `FAO_codebook' 
	drop if _m!=3
	drop _m
	drop if unit == "kcal/capita/day" & covariate != "total" 
	
	// drop observations where value is missing so that when summing by covariate, overall estimate does not go to 0
		drop if value == . 
		
	// Aggregate all values by summing them across country-years into buckets for each different covariate
		collapse (sum) value, by(country year covariate) fast
		rename country countryname

	// fix naming of whole grains to refined grains
		replace covariate = "refined_grains" if covariate == "whole_grains"

	 save `out_dir'`fao_file'.dta, replace
}

if `prep_data' !=1 {
	use `out_dir'`fao_file'.dta, replace
}

*replace sweet potatoe with sweet_potato

replace covariate = "sweet_potato" if covariate == "sweet potato"

**rename countrynames to match ihme location_names
rename countryname location_name
**remove "China" because it is a combination of ALL of China
drop if location_name == "China"
replace location_name = "Russia" if location_name == "Russian Federation"
replace location_name = "Vietnam" if location_name == "Viet Nam"
replace location_name = "United States" if location_name == "United States of America"
replace location_name = "Iran" if location_name == "Iran (Islamic Republic of)"
replace location_name = "China (without Hong Kong and Macao)" if location_name == "China, mainland"
replace location_name = "Venezuela" if location_name == "Venezuela (Bolivarian Republic of)"
replace location_name = "Tanzania" if location_name == "United Republic of Tanzania"
replace location_name = "Taiwan" if location_name == "China, Taiwan Province of"
replace location_name = "Syria" if location_name == "Syrian Arab Republic"
replace location_name = "South Korea" if location_name == "Republic of Korea"
replace location_name = "North Korea" if location_name == "Democratic People's Republic of Korea"
replace location_name = "Cote d'Ivoire" if location_name == "Côte d'Ivoire"
replace location_name = "Cape Verde" if location_name == "Cabo Verde"
replace location_name = "Bolivia" if location_name == "Bolivia (Plurinational State of)"
replace location_name = "Macedonia" if location_name == "The former Yugoslav Republic of Macedonia"
replace location_name = "Brunei" if location_name == "Brunei Darussalam"
replace location_name = "Laos" if location_name == "Lao People's Democratic Republic"
replace location_name = "The Bahamas" if location_name == "Bahamas"
replace location_name = "The Gambia" if location_name == "Gambia"
replace location_name = "Moldova" if location_name == "Republic of Moldova"
replace location_name = "Hong Kong Special Administrative Region of China" if location_name == "China, Hong Kong SAR"
replace location_name = "Macao Special Administrative Region of China" if location_name == "China, Macao SAR"
replace location_name = "Ethiopia" if location_name == "Ethiopia PDR"
replace location_name = "Sudan" if location_name == "Sudan (former)"
rename location_name countryname
	

	merge m:1 countryname using `locations'
	keep if _merge == 3
	drop _merge

**this is where the nutrient data should be appended in
preserve
	use "FILEPATH/`version'/FBS_USDA_nutrients_`version'.dta", clear
	rename location_name countryname
	rename mean value
	**removing "diet_" to create the covariate variable
	gen covariate = substr(ihme_risk, 6, .)
	replace covariate = "saturated_fats" if covariate == "satfat"
	gen nutrients_data = 1
	drop risk
	tempfile nutrients
	save `nutrients', replace
restore

**gather the energy covariate for use in adjusting the sales data
preserve
**bring in modeled FAO energy estimates
import delimited "$FILEPATH/energy_kcal_unadj_2.csv", clear
	duplicates drop
	rename year_id year
	rename gpr_mean energy
	keep location_id year energy
	tempfile energy_cov
	save `energy_cov', replace
restore

**this is where the sales data should be appended in
preserve
	use "FILEPATH/sales_data_outliered_`sales_version'.dta", clear
	rename location_name countryname
	rename mean value
	**removing "diet_" to create the covariate variable
	gen covariate = substr(ihme_risk, 6, .)
	gen sales_data = 1
	drop risk
	**associate energy from nutrients calculations
	merge m:1 location_id year using `energy_cov' 
		keep if _merge == 3
		drop _merge
	**create an energy correction factor to be used in adjusting the energy estimate that will be coming from FAO data
	gen energy_sales_corr_factor = 2459/3281 //this is a global scalar that will be applied to FAO energy estimates to make them comparable to household budget survey energy estimates (3281 = FAO energy estimate, 2459 = HHBS energy estimate); extracted from Serra-Majem et al. 2003, Comparative analysis of nutrition data from national, household, and individual levels: results from a WHO-CINDI collaborative project in Canada, Finland, Poland, and Spain
	replace energy = energy * energy_sales_corr_factor
		drop energy_sales_corr_factor

	**prepare a separate adjustment for hydrogenated vegetable oil
	replace value = value * 9 / energy if ihme_risk == "diet_hvo_sales"
	replace case_definition = "% of total dietary energy" if ihme_risk == "diet_hvo_sales"

	**create a scalar that will be used in energy adjusting the data
	replace energy_adj_scalar = 2000 / energy
		drop energy

	tempfile sales
	save `sales', replace
restore

// restoring the FBS data 		
// Want to adjust so that all measurements are equivalent to 2000 kcal/day consumption
	preserve
	// break out the total kcal per day food availability data
	keep location_name location_id covariate value year
	
	keep if covariate == "total"
	

	gen energy_adj_scalar = .
	gen sales_energy_adj_scalar = .
	
	// 2000 calories is the reference diet - all country-years with total diet higher than this level should be scaled down, and vice versa for those that are lower
	
		replace energy_adj_scalar = 2000 / value
		
	// Generate a total diet calories variable to merge onto everything else as a comparison
	
		generate total_calories = value
		drop value
	
	tempfile energy_adj_scalar_data
	save `energy_adj_scalar_data', replace
	
	restore
    merge m:1 location_id year using `energy_adj_scalar_data', keep(3) 
	drop _m

	**remove the saturated fats created based solely off of total food items
	drop if covariate == "saturated_fats"

	**this is where nutrient & sales data should be appended in with the energy scalar already calculated...
	append using `nutrients'
	append using `sales'

	**fixing NID
	replace nid = 239249 if nutrients_data == 1
	replace nid = 200195 if nutrients_data != 1 & sales_data != 1
	replace nid = 282698 if sales_data == 1

// Convert kilograms per year into grams per day and apply the scalar to get 2000 kcal/day standardized value
	generate grams_daily = .
	replace grams_daily = value * 1000 / 365 * energy_adj_scalar if nutrients_data != 1 & covariate != "total"  & sales_data != 1
	replace grams_daily = value * energy_adj_scalar if nutrients_data == 1 | sales_data == 1
	replace grams_daily = . if covariate == "energy" | covariate == "total"
	replace grams_daily = . if grams_daily == 0
	**hydrogenated vegetable oil has already been adjusted
	replace grams_daily = value if sales_data == 1 & covariate == "hvo_sales"
// Convert kilograms per year into grams per day without adjustment for comparison
	gen grams_daily_unadj = .
	replace grams_daily_unadj = value * 1000 / 365 if nutrients_data != 1 & covariate != "total" 
	replace grams_daily_unadj = value if nutrients_data == 1 | sales_data == 1

	replace grams_daily_unadj = . if grams_daily_unadj == 0

// Drop no longer needed vars
	drop energy_adj_scalar
	
// Final modifications and export
	gen iso3 = ihme_loc_id
	drop if iso3 == "PSE" 
	rename covariate gbd_cause
	gen risk = gbd_cause
	gen countryname_ihme = countryname
	gen ihme_country = countryname

**removing obvious outliers from data

	drop if iso3 == "MNE" & nutrients_data == 1
	drop nutrients_data
	
	compress
	
	order iso3 countryname_ihme year gbd_cause grams*
	save "FILEPATH/`version/`version'/FAO_all_`version'.dta", replace
	
//save a subset of the file that has the location information for variance estimation (used in paralellizing on cluster)
keep iso3 location_name location_id ihme_loc_id
duplicates drop
    save "FILEPATH/`version'/FAO_locyears.dta", replace

