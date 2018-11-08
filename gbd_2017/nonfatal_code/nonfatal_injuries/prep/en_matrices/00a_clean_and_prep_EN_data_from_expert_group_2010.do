// Cleans hospital data from the Expert Group GBD 2010

// Prep stata
	clear
	set more off, perm

// Import macros
	global prefix `1'
	local inj_dir `2'
	local prepped_dir `3'
	local ages `4'
	local code_dir `5'

	di "$prefix"
	di "`inj_dir'"
	di "`prepped_dir'"
	di "`ages'"
	di "`code_dir'"

// Set other macros
	local country_codes "FILEPATH.DTA"
	local map_dir "`inj_dir'/FILEPATH"
	local en_map "`map_dir'/FILEPATH.xls"
	
// Import params
	adopath + `code_dir'/ado
	adopath + "FILEPATH"
	load_params
	
// Set up necessary maps
	// Country name spellings
	use "`country_codes'", clear
	rename name countryname
	keep countryname location_id
	duplicates drop
	tempfile alt_spell
	save `alt_spell'
	
	// Country name to iso3
	get_location_metadata, location_set_id(35) clear
	keep location_id ihme_loc_id
	rename ihme_loc_id iso3

	merge 1:m location_id using `alt_spell', keep(match) nogen
	tempfile name_iso3_map
	save `name_iso3_map'
	
	// E/N map for expert group data
	import excel ext_code=D e_code=G using "`en_map'", sheet("e_code") cellrange(A2) clear
	drop if e_code == ""
	tempfile e_map
	save `e_map'
	
	import excel seq_code=A n_code=H using "`en_map'", sheet("n_code") cellrange(A2) clear
	drop if n_code == ""
	tempfile n_map
	save `n_map'

// Import data
	import delimited using "`inj_dir'/FILEPATH.csv", delim(",") clear
	keep countryname age sex ext_code seq_code cases outcome
	
	** fix age-group naming
	gen dash_ix = strpos(age,"-")
	replace dash_ix = 3 if age == "85+"
	gen age_num = substr(age,1,dash_ix-1)
	drop dash_ix
	destring age_num, replace
	replace age_num = 0 if age == "<1"
	replace age_num = . if age == "Unknown"
	drop age
	rename age_num age
	local num_ages = wordcount("`ages'") - 1
	forvalues x = 1/`num_ages' {
		local start_age = word("`ages'",`x')
		local end_age = word("`ages'",`x'+1)
		replace age = `start_age' if age >= `start_age' & age < `end_age'
	}
	local final_age = word("`ages'",`num_ages'+1)
	replace age = `final_age' if age >= `final_age'
	
	** encode sex
	rename sex sextext
	gen sex = .
	replace sex = 1 if sextext == "Male"
	replace sex = 2 if sextext == "Female"
	drop sextext
	
// Map to iso3 & clean
	merge m:1 countryname using `name_iso3_map', assert(match using)
	keep if _m == 3
	drop _m
	drop countryname
	
	// Gen inpatient variable
	assert outcome != ""
	gen inpatient = 0
	replace inpatient = 1 if outcome == "inpatient"
	drop outcome
	
// Merge on E/N mapping
	merge m:1 ext_code using `e_map', keep(match) nogen
	merge m:1 seq_code using `n_map', keep(match) nogen
	
// drop any "too aggregated" N-codes
levelsof e_code, l(e_codes) c
foreach e of local e_codes {
	local drop 1
	foreach f of global modeled_e_codes {
		if regexm("`e'","`f'") local drop 0
	}
	if `drop' drop if e_code == "`e'"
}
	
// Save desired variables
	keep iso3 location_id age sex inpatient e_code n_code cases ext_code seq_code
	export delimited using "`prepped_dir'/FILEPATH.csv", delim(",") replace
	