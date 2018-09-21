** Parallel create aggs for envelope


clear all
set more off
cap restore, not

if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local sim "`1'"
		local in_dir "`2'"
		local map_dir "`3'"
		local out_dir "`4'"
		adopath + "FILEPATH"
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		local sim 1
		local in_dir "FILEPATH"
		local map_dir "FILEPATH"
		local out_dir "FILEPATH"
		adopath + "FILEPATH"
	}




insheet using "`map_dir'/super_map.csv", clear
tempfile super_map
save `super_map'

insheet using "`map_dir'/region_map.csv", clear
drop location_id
tempfile region_map
save `region_map'

insheet using "`map_dir'/lowest_map.csv", clear
tempfile lowest_map
save `lowest_map'

insheet using "`map_dir'/agemap_all.csv", clear
tempfile age_map
save `age_map'

insheet using "`map_dir'/locs_all.csv", clear
tempfile locs_all
save `locs_all'

// Grab population #s
	import delimited "FILEPATH/env_pop.csv", clear
	rename population pop
	merge m:1 location_id using `locs_all'
	drop _merge
	merge m:1 age_group_id using `age_map'
	drop if _merge==2
	drop _merge
	keep pop sex_id age_group_name age_group_id ihme_loc_id year_id region_id
	
	levelsof region_id if region_id != "NA", local(regions)
	drop region_id
	tempfile pop_final
	rename year year
	drop if ihme_loc_id=="" 
	save `pop_final'
	
	insheet using "`map_dir'/agemap.csv", clear
	split age_group_name, parse(" to ")
	rename age_group_name1 age
	keep age age_group_id
	
	tempfile age_groups
	save `age_groups'
	
	clear 
	foreach r of local regions {
		foreach s in "male" "female" {
			append using "`in_dir'/`s'_`r'/envelope_`r'_`s'_sim_`sim'.dta"
		}
	}
	
	
	preserve
	keep ihme_loc_id
	duplicates drop
	di _N
	assert _N == 754 
	restore
	
	gen sex_id = 1 if sex == "male"
	replace sex_id = 2 if sex == "female"
	
	replace age = "95 plus" if age == "95"
	replace age = "Early Neonatal" if age == "enn"
	replace age = "Late Neonatal" if age == "lnn"
	replace age = "Post Neonatal" if age == "pnn"
	replace age = "<1 year" if age == "0"
	
	merge m:1 age using `age_groups'
	assert _m!=1
	keep if _m==3
	drop _m
	drop if year < 1950
	merge 1:1 ihme_loc_id age_group_id sex_id year using `pop_final'
	assert _m!=1
	keep if _m == 3
	drop _m
	tempfile envelope
	
	
	save `envelope'
	by ihme_loc_id, sort: gen nvals = _n == 1
	egen var = sum(nvals)
	local n = var[1]
	drop nvals var
	

  import delimited "FILEPATH/locations.csv", clear
	keep ihme_loc_id level parent_id location_id
	merge 1:m ihme_loc_id using `envelope'  
	drop if _merge==1  	
	assert _m!=2
	drop _merge
	
	replace level=3 if ihme_loc_id== "CHN_354" | ihme_loc_id == "CHN_361"

	set type double 
	gen scaled_envelope = enve  
	save `envelope', replace
	

	use `envelope', clear
	keep if level ==3 
	keep location_id sex year age_group_name scaled_envelope ihme_loc_id
	rename location_id parent_id 
	rename scaled_envelope parent_envelope
	rename ihme_loc_id parent_ihme_loc_id
	tempfile three
	save `three'
	

	use `envelope', clear
	keep if level == 4
	tempfile four
	save `four'
	collapse (sum) scaled_envelope, by(parent_id sex year age_group_name)
	rename scaled_envelope agg_env_4
	
	merge m:1 parent_id sex year age_group_name using `three' 
	drop if _merge != 3
	drop _merge
	gen scale_4 = parent_envelope/agg_env_4
	
	tempfile scale_4 
	save `scale_4'
	
	use `envelope', clear
	merge m:1 parent_id sex year age_group_name using `scale_4' 
	drop _merge parent_ihme_loc_id parent_envelope agg_env_4
	replace scaled_envelope = scaled_envelope*scale_4 if !missing(scale_4) & substr(ihme_loc_id,1,3) != "ZAF"
	save `envelope', replace
	 
	use `envelope', clear
	keep if level == 4
	tempfile four
	keep location_id sex year age_group_name scaled_envelope ihme_loc_id
	rename location_id parent_id 
	rename scaled_envelope parent_envelope
	rename ihme_loc_id parent_ihme_loc_id
	save `four', replace
	
	use `envelope', clear
	keep if level == 5
	tempfile five
	collapse (sum) scaled_envelope, by(parent_id sex year age_group_name)
	rename scaled_envelope agg_env_5
	save `five'
	
	merge m:1 parent_id sex year age_group_name using `four' 
	drop if _merge != 3
	drop _merge
	gen scale_5 = parent_envelope/agg_env_5
	
	tempfile scale_5 
	save `scale_5'
	
	use `envelope', clear
	merge m:1 parent_id sex year age_group_name using `scale_5' 
	drop _merge parent_ihme_loc_id parent_envelope agg_env_5
	replace scaled_envelope = scaled_envelope*scale_5 if !missing(scale_5) & substr(ihme_loc_id,1,3) != "ZAF"
	save `envelope', replace

	use `envelope', clear
	keep if level == 5
	tempfile five
	keep location_id sex year age_group_name scaled_envelope ihme_loc_id
	rename location_id parent_id 
	rename scaled_envelope parent_envelope
	rename ihme_loc_id parent_ihme_loc_id
	save `five', replace
	

	use `envelope', clear
	keep if level == 6
	tempfile six
	collapse (sum) scaled_envelope, by(parent_id sex year age_group_name)
	rename scaled_envelope agg_env_6
	save `six'
	

	merge m:1 parent_id sex year age_group_name using `five' 
	drop if _merge != 3
	drop _merge
	gen scale_6 = parent_envelope/agg_env_6
	
	tempfile scale_6 
	save `scale_6'
	
	use `envelope', clear
	merge m:1 parent_id sex year age_group_name using `scale_6' 
	drop _merge parent_ihme_loc_id parent_envelope agg_env_6
	replace scaled_envelope = scaled_envelope*scale_6 if !missing(scale_6) & substr(ihme_loc_id,1,3) != "ZAF"
	save `envelope', replace
	
	drop scale_4 scale_5 scale_6
	rename scaled_envelope envelope_scaled_`sim'
	
	drop if ihme_loc_id == "ZAF"
	preserve
	keep if substr(ihme_loc_id,1,3) == "ZAF" & ihme_loc_id != "ZAF"
	collapse (sum) envelope_scaled_`sim' envelope_with* pop, by(parent_id sex year age sex_id age_group_id age_group_name )
	gen ihme_loc_id = "ZAF"
	rename parent_id location_id
	gen parent_id = 192
	gen level = 3
	tempfile addzaf
	save `addzaf', replace
	restore
	append using `addzaf'
	
	
	merge m:1 ihme_loc_id using `lowest_map'
	preserve
	keep if _m == 2 & (substr(ihme_loc_id,1,3) == "GBR" | substr(ihme_loc_id,1,3) == "CHN" | substr(ihme_loc_id,1,3) == "IND")
	keep location_id
	rename location_id parent_id
	tempfile parents
	save `parents', replace
	restore
	keep if _m == 3
	drop _m
	
	preserve
	merge m:1 parent_id using `parents'
	keep if _m == 3
	drop _m
	collapse (sum) pop enve*, by(parent_id sex year age sex_id age_group_id age_group_name)
	rename parent_id location_id
	merge m:1 location_id using `lowest_map'
	keep if _m == 3
	drop _m
	tempfile toadd
	save `toadd', replace
	restore
	append using `toadd'
	
	tempfile compiled	
	save `compiled', replace

	insheet using "`map_dir'/region_map.csv", clear
	keep location_id
	rename location_id parent_id
	merge 1:m parent_id using `compiled'
	keep if _m == 3
	drop _m
	collapse (sum) pop enve*, by(parent_id sex year age sex_id age_group_id age_group_name) fast
	rename parent_id location_id
	merge m:1 location_id using `lowest_map'
	keep if _m == 3
	drop _m 
	tempfile region
	save `region', replace
	collapse (sum) pop enve*, by(parent_id sex year age sex_id age_group_id age_group_name) fast
	rename parent_id location_id
	merge m:1 location_id using `lowest_map'
	keep if _m == 3
	drop _m
	tempfile super
	save `super', replace
	collapse (sum) pop enve*, by(parent_id sex year age sex_id age_group_id age_group_name) fast
	rename parent_id location_id
	merge m:1 location_id using `lowest_map'
	keep if _m == 3
	drop _m
	append using `super'
	append using `region'
	append using `compiled'

	preserve
	collapse (sum) pop enve*, by(location_id parent_id year age age_group_id age_group_name location_name ihme_loc_id path_to_top_parent) fast
	gen sex = "both"
	gen sex_id = 3
	tempfile bothsex
	save `bothsex', replace
	restore
	append using `bothsex'
	
	preserve 
	drop if age == "<1 year"
	collapse (sum) pop enve*, by(location_id parent_id year sex sex_id location_name ihme_loc_id path_to_top_parent) fast
	gen age = "All Ages"
	gen age_group_id = 22
	gen age_group_name = "All Ages"
	tempfile allage
	save `allage', replace
	restore
	append using `allage'
	
	drop parent_id path_to_top_parent
	
	assert age != ""
	assert age_group_name != ""
	assert age_group_id != .
	assert sex != "" 
	assert year != .
	assert sex_id != .
	assert pop != .
	assert ihme_loc_id != ""
	assert location_id != .
	assert envelope_withhiv != . if year >= 1970
	assert envelope_scaled != . if year >= 1970
	
	** rename envelope draw to make sure it's named without second sim
	rename envelope_withhiv env_unscaled_`sim'
	rename envelope_scaled env_`sim'
	
	saveold "`out_dir'/envelope_sim_`sim'.dta", replace
	
	
