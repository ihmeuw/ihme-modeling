*****************************************************************************************************************************
**  Description: This script compiles all of the location level nutrient composition data into one csv. Fats are turned into percents here. 

****************************************************************************************************************************
** Set preferences for STATA
			cap restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

**************************************************************************
******** Begin formatting the dataset                         ************
**************************************************************************

local version "GBD2021"
local version_description "GBD2021" 

adopath + "FILEPATH"

**call in arguments being passed in
args version

local output_f "FILEPATH/indiv_nutrients/"
capture confirm file `output_f'
if _rc mkdir `output_f'



use "FILEPATH/compiled_nutrients_'version'.dta", clear


local nutrients "calcium_mg_sum sodium_mg_sum fiber_g_sum pufa_g_sum tfa_g_sum omega3_g_sum satfat_g_sum energy_kcal_sum cholesterol_mg_sum zinc_mg_sum vit_a_retinol_ug_sum vit_a_iu_sum vit_a_rae_ug_sum protein_g_sum iron_mg_sum mufa_g_sum folic_acid_ug_sum magnesium_mg_sum phosphorus_mg_sum potassium_mg_sum selenium_ug_sum starch_g_sum sugars_g_sum total_carbohydrates_g_sum total_fats_g_sum folates_dfe_ug_sum vit_k_ug_sum alcohol_g_sum"


global count = 0


foreach nutrient of local nutrients {
	preserve
	keep countries location_id year `nutrient'
	gen risk = "`nutrient'"
	rename `nutrient' mean 

if ($count == 0) {
	save "FILEPATH/indiv_gen_nutrients_'version'.dta", replace
	global count = $count + 1
}
else {
	tempfile country_year_data
	save `country_year_data', replace
	use "FILEPATH/indiv_gen_nutrients_'version'.dta", clear
	append using `country_year_data'
	save "FILEPATH/indiv_gen_nutrients_'version'.dta", replace
	global count = $count + 1
}
	restore
}

use "FILEPATH/indiv_gen_nutrients_'version'.dta", clear

**let's merge in total grains here

preserve
import delimited "FILEPATH/grain_contributors.csv", clear

	keep if grain == 1

	keep product_codes

	tempfile grain_codes
	
	save `grain_codes', replace
	
	

use "FILEPATH/data.dta", clear

	merge m:1 product_codes using `grain_codes', assert(1 3) keep(3) nogen

	drop if data == .

	collapse (sum) data, by(countries year)
	
	gen risk = "total_grains"

	rename data mean

	tempfile total_grains
	save `total_grains', replace

restore

**add in total grains
append using `total_grains'

drop if mean == 0

**need year_start, year_end
gen year_start = year
gen year_end = year

**need modelable_entity_id

rename countries location_name
	**using central function to get country-location_id pairs
preserve
	get_location_metadata, location_set_id(35) clear
	**making location_name unique
	drop if location_name == "Distrito Federal"
	drop if location_name == "North Africa and Middle East"
	drop if location_name == "South Asia"
	drop if location_name == "Niger" & level == 4
	drop if location_name == "Punjab" & level == 4
	drop if location_id == 533
	drop if level < 3
	keep location_id level location_name super_region_name region_name ihme_loc_id

	tempfile ihme_locations
	save `ihme_locations', replace
	save "FILEPATH/test_of_ihme_locations.dta", replace

	restore

* rename locations to match with GBD location naming style

replace location_name = "Russia" if location_name == "Russian Federation"
replace location_name = "Vietnam" if location_name == "Viet Nam"
replace location_name = "United States" if location_name == "USA" //
replace location_name = "United Kingdom" if location_name == "UK" //
replace location_name = "Iran" if location_name == "Iran (Islamic Republic of)"
replace location_name = "Palestine" if location_name == "Palestine occupied"
replace location_name = "Saint Kitts and Nevis" if location_name == "St Kitts Nev"
replace location_name = "China (without Hong Kong and Macao)" if location_name == "China main" //
replace location_name = "China" if location_name == "China main" //
replace location_name = "Venezuela" if location_name == "Venezuela (Bolivarian Republic of)"
replace location_name = "South Korea" if location_name == "Korea Rep" //
replace location_name = "North Korea" if location_name == "Democratic People's Republic of Korea"
replace location_name = "Cote d'Ivoire" if location_name == "Côte d'Ivoire"
replace location_name = "Bolivia" if location_name == "Bolivia (Plurinational State of)"
replace location_name = "Brunei" if location_name == "Brunei Darsm" //
replace location_name = "The Bahamas" if location_name == "Bahamas"
replace location_name = "The Gambia" if location_name == "Gambia"
replace location_name = "Hong Kong Special Administrative Region of China" if location_name == "China HK" //
replace location_name = "Macao Special Administrative Region of China" if location_name == "China Mac" //
replace location_name = "Taiwan" if location_name == "China Tai" //
replace location_name = "United Arab Emirates" if location_name == "Untd Arab Em" //
replace location_name = "Bosnia and Herzegovina" if location_name == "Bosnia Herzg" //
replace location_name = "Czech Republic" if location_name == "Czech Rep" //
replace location_name = "Papua New Guinea" if location_name == "Papua N Guinea" //
replace location_name = "Guinea-Bissau" if location_name == "GuineaBissau" //
replace location_name = "Laos" if location_name == "Lao People's Democratic Republic of" //
replace location_name = "Sao Tome and Principe" if location_name == "Sao Tome Prn" //
replace location_name = "Central African Republic" if location_name == "Cent Afr Republic" //
replace location_name = "Democratic Republic of the Congo" if location_name == "Congo Dem Republic" //
replace location_name = "Congo" if location_name == "Congo; Rep" //
replace location_name = "Trinidad and Tobago" if location_name == "Trinidad Tob" //
replace location_name = "Sudan" if location_name == "Sudan (former)" //
replace location_name = "Solomon Islands" if location_name == "Solomon Is" //
replace location_name = "Antigua and Barbuda" if location_name == "Antigua Barb" //
replace location_name = "Moldova" if location_name == "Moldova Rep" //
replace location_name = "Saint Lucia" if location_name == "St Lucia" //
replace location_name = "Saint Vincent and the Grenadines" if location_name == "St Vincent" //
replace location_name = "Fiji" if location_name == "Fiji Islands" //

**simply duplicate UK for each of its countries within

expand 2 if location_name == "United Kingdom", gen(dup)
	replace location_name = "England" if dup == 1
	replace location_id = "4749" if dup== 1
		drop dup
expand 2 if location_name == "United Kingdom", gen(dup)
	replace location_name = "Scotland" if dup == 1
	replace location_id = "434" if dup== 1
		drop dup
expand 2 if location_name == "United Kingdom", gen(dup)
	replace location_name = "Wales" if dup == 1
	replace location_id = "4636" if dup== 1
		drop dup
	replace location_name = "Northern Ireland" if location_name == "United Kingdom"
	replace location_id = "433" if location_name == "Northern Ireland"

	
	save "FILEPATH/test_of_data_with_locations.dta", replace
    merge m:1 location_name using `ihme_locations', keep(3) nogen

gen nid = 203327

**need svy (FAO/USDA Nutrients)

gen svy = "FAO_SUA_USDA_SR Nutrients"

**need metc (unique to the risk), a common identifier throughout the diet process
gen metc = .
replace metc = 20 if risk == "calcium_mg_sum"
replace metc = 18 if risk == "fiber_g_sum"
replace metc = 22 if risk == "omega3_g_sum"
replace metc = 13 if risk == "pufa_g_sum"
replace metc = 16 if risk == "tfa_g_sum"
replace metc = 19 if risk == "sodium_mg_sum"
replace metc = 12 if risk == "satfat_g_sum"

**need data_status (should be "active")
gen data_status = "active"

**need ihme_risk
gen ihme_risk = "."
replace ihme_risk = "diet_calcium" if risk == "calcium_mg_sum"
replace ihme_risk = "diet_fiber" if risk == "fiber_g_sum"
replace ihme_risk = "diet_omega_3" if risk == "omega3_g_sum"
replace ihme_risk = "diet_pufa" if risk == "pufa_g_sum"
replace ihme_risk = "diet_transfat" if risk == "tfa_g_sum"
replace ihme_risk = "diet_salt" if risk == "sodium_mg_sum"
replace ihme_risk = "diet_satfat" if risk == "satfat_g_sum"
replace ihme_risk = "diet_cholesterol" if risk == "cholesterol_mg_sum"
replace ihme_risk = "diet_energy" if risk == "energy_kcal_sum"
replace ihme_risk = "diet_vit_a_retinol" if risk == "vit_a_retinol_ug_sum"
replace ihme_risk = "diet_vit_a_rae" if risk == "vit_a_rae_ug_sum"
replace ihme_risk = "diet_vit_a_iu" if risk == "vit_a_iu_sum"
replace ihme_risk = "diet_zinc" if risk == "zinc_mg_sum"
replace ihme_risk = "diet_protein" if risk == "protein_g_sum"
replace ihme_risk = "diet_iron" if risk == "iron_mg_sum"
replace ihme_risk = "diet_mufa" if risk == "mufa_g_sum"
replace ihme_risk = "diet_folic_acid" if risk == "folic_acid_ug_sum"
replace ihme_risk = "diet_folates" if risk == "folates_ug_sum"
replace ihme_risk = "diet_magnesium" if risk == "magnesium_mg_sum"
replace ihme_risk = "diet_phosphorus" if risk == "phosphorus_mg_sum"
replace ihme_risk = "diet_potassium" if risk == "potassium_mg_sum"
replace ihme_risk = "diet_selenium" if risk == "selenium_ug_sum"
replace ihme_risk = "diet_carbohydrates" if risk == "total_carbohydrates_g_sum"
replace ihme_risk = "diet_fats" if risk == "total_fats_g_sum"
replace ihme_risk = "diet_starch" if risk == "starch_g_sum"
replace ihme_risk = "diet_sugar_sua" if risk == "sugars_g_sum"
replace ihme_risk = "diet_folates_dfe" if risk == "folates_dfe_ug_sum"
replace ihme_risk = "diet_vit_k" if risk == "vit_k_ug_sum"
replace ihme_risk = "diet_alcohol" if risk == "alcohol_g_sum"
replace ihme_risk = "diet_total_grains" if risk == "total_grains"

**change zinc to g from mg
replace mean = mean / 1000 if risk == "zinc_mg_sum"
	replace risk = "zinc_g_sum" if risk == "zinc_mg_sum"

**need me_risk (should be same as ihme_risk)
gen me_risk = ihme_risk

**need representative_name (likely "Nationally representative only")
gen representative_name = "Nationally representative only"

**need is_outlier
gen is_outlier = 0

**need case_definition (unique to cause)
gen case_definition = "."
	**need to chance some datapoints before applying g/day case def to all
	**going from mg to g where appropriate
	replace mean = mean / 1000 if inlist(ihme_risk, "diet_calcium")
	replace risk = "calcium_g_sum" if risk == "calcium_mg_sum"
replace case_definition = "g/day"
replace case_definition = "mg/day" if regexm(risk, "mg")
replace case_definition = "kcal/day" if ihme_risk == "diet_energy"
replace case_definition = "ug/day" if regexm(risk, "ug") & ihme_risk != "diet_sugar_sua"
replace case_definition = "iu/day" if ihme_risk == "diet_vit_a_iu"

**need modelable_entity_name and modelable_entity_id
gen modelable_entity_name = "."
gen modelable_entity_id = .

replace modelable_entity_name = "Diet suboptimal in calcium (g/day)" if metc == 20
replace modelable_entity_id = 2427 if metc == 20

replace modelable_entity_name = "Diet low in fiber (g/day)" if metc == 18
replace modelable_entity_id = 2428 if metc == 18

replace modelable_entity_name = "Diet low in omega-3 fatty acids (g/day)" if metc == 22
replace modelable_entity_id = 2429 if metc == 22

replace modelable_entity_name = "Diet low in polyunsaturated fatty acids (g/day)" if metc == 13
replace modelable_entity_id = 2436 if metc== 13

replace modelable_entity_name = "Diet high in sodium (g/day)" if metc == 19
replace modelable_entity_id = 2438 if metc == 19

replace modelable_entity_name = "Diet high in saturated fatty acids (g/day)" if metc == 12
replace modelable_entity_id = 2439 if metc == 12

replace modelable_entity_name = "Diet high in trans fatty acids (g/day)" if metc == 16
replace modelable_entity_id = 2441 if metc == 16

**need cv_natl_rep (should be 1 in all these instances)
gen cv_natl_rep = 1

**adding covariate to state that is this FAO data
gen cv_fao_data = 1

**generating a new variable for those calculated in GBD as "% of total dietary energy"
preserve
	keep if risk == "energy_kcal_sum"
	keep location_name year mean risk location_id
	rename mean energy
	duplicates drop
	tempfile energy_values
	save `energy_values', replace
restore

merge m:1 location_name year using `energy_values'
keep if _merge == 3
drop _merge

** this is where/how we convert fats over to % daily energy!
**create the energy scalar
gen energy_adj_scalar = .
replace energy_adj_scalar = 2000 / energy
gen total_calories = energy

preserve
	keep if inlist(ihme_risk, "diet_pufa", "diet_transfat", "diet_satfat", "diet_mufa", "diet_fats")

	**there are 9 kcal in every gram of fat (going from g to kcal)
	replace mean = mean * 9
	**this turns these into % as we would like them for the purposes of GBD
	replace mean = mean / energy
	replace case_definition = "% of total dietary energy"
	replace modelable_entity_name = "Diet high in saturated fatty acids (% of total dietary energy)" if metc == 12
	replace modelable_entity_name = "Diet high in trans fatty acids (% of total dietary energy)" if metc == 16
	replace modelable_entity_name = "Diet low in polyunsaturated fatty acids (% of total dietary energy)" if metc == 13
	tempfile fats_energy_units
	save `fats_energy_units', replace
restore

****PREPARE TEMPFILES FOR EACH OF THE CARBOHYDRATE CATEGORIES*********
preserve
	keep if ihme_risk == "diet_carbohydrates"
	**there are 4 kcal in every gram of carb
	replace mean = mean * 4

	**this turns these into % as we would like them for the purposes of GBD
	replace mean = mean / energy 

	**keep location_name year mean

	replace case_definition = "% of total dietary energy"
	tempfile carbs_energy_units
	save `carbs_energy_units', replace
restore

preserve
	keep if ihme_risk == "diet_starch"
	**there are 4 kcal in every gram of carb
	replace mean = mean * 4

	**this turns these into % as we would like them for the purposes of GBD
	replace mean = mean / energy 

	**keep location_name year mean

	replace case_definition = "% of total dietary energy"
	tempfile starch_energy_units
	save `starch_energy_units', replace
restore


preserve
	keep if inlist(ihme_risk, "diet_protein")
	**there are 4 kcal in every gram of protein
	replace mean = mean * 4
	replace mean = mean / energy 
	replace case_definition = "% of total dietary energy"
	tempfile protein_energy_units
	save `protein_energy_units', replace
restore

	drop if inlist(ihme_risk, "diet_pufa", "diet_satfat", "diet_transfat", "diet_mufa", "diet_fats", "diet_carbohydrates" ,"diet_starch","diet_protein") 

**append in the fats that were turned into % of total energy
append using `fats_energy_units'
append using `carbs_energy_units'
*append using `sugar_energy_units'
append using `starch_energy_units'
append using `protein_energy_units'
*append using `alcohol_energy_units'


save "/FILEPATH/FBS_USDA_nutrients_'version'.dta", replace


