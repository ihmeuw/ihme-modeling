// Purpose: Custom_code to prep WaSH and HAP indicators for tabulation

//Read in ratios to split ambiguous strings
preserve
	//Sanitation ratios
	 import delimited "FILEPATH", clear
	 tempfile sani_cw
	 save `sani_cw', replace
	 keep if iso3 == substr("$ihme_loc_id", 1, 3)
	 local region = reg
	 if sources < 5 {
	 	use `sani_cw', clear
	 	keep if reg == "`region'"
	 	collapse (sum) latrine_imp latrine_unimp flush_imp flush_unimp
	 	save `sani_cw', replace
	 }
	 else {
	 	save `sani_cw', replace
	 }

	//Water ratios
	 import delimited "FILEPATH", clear
	 tempfile water_cw
	 save `water_cw', replace
	 keep if iso3 == substr("$ihme_loc_id", 1, 3)
	 local region = reg
	 if sources < 5 {
	 	use `water_cw', clear
	 	keep if reg == "`region'"
	 	collapse (sum) spring_imp spring_unimp well_imp well_unimp piped piped_imp
	 	save `water_cw', replace
	 }
	 else {
	 	save `water_cw', replace
	 }
restore

local hh_size_adj = 1
local shared = 0 // we're not modeling shared sanitation specification since it's not considered in relative risk studies

// Construct handwashing indicator
	cap confirm variable hw_soap hw_station hw_water
	if !_rc {
		di in red "creating handwashing indicator..."
		gen wash_hwws = 0 if hw_soap == 1 & hw_station == 1 & hw_water == 1
		replace wash_hwws = 1 if wash_hwws == .
	}

// Improved and piped water indicator generation
	cap confirm variable w_source_drink_mapped
	if !_rc {
		di as error "creating water indicator..."
		gen water = 0 if mi(w_source_drink_mapped) | w_source_drink_mapped == "other" | w_source_drink_mapped == "unknown"
		replace water = 1 if water != 0

		//piped water indicator
		gen wash_water_piped = 1 if w_source_drink_mapped == "piped"
		replace wash_water_piped = 0 if wash_water_piped != 1 & water == 1

		//improved water, with non-piped water population as the denominator
		gen wash_water_imp_prop = 1 if w_source_drink_mapped == "improved" | w_source_drink_mapped == "well_imp" | w_source_drink_mapped == "bottled" | w_source_drink_mapped == "spring_imp" | w_source_drink_mapped == "piped_imp"
		replace wash_water_imp_prop = 0 if water == 1 & wash_water_piped != 1 & wash_water_imp_prop != 1
		tempfile master
		save `master', replace

		//snag crosswalking ratios from prepped tempfile
		use `water_cw', clear

			//proportion of the ambiguous well string that is "improved"
			gen well_imp_rat = well_imp/(well_unimp + well_imp)
			local well_imp_prop = well_imp_rat

			//proportion of the ambiguous spring string that is "improved"
			gen spring_imp_rat = spring_imp/(spring_unimp + spring_imp)
			local spring_imp_prop = spring_imp_rat

			//proportion of the ambiguous spring string that is "improved"
			gen piped_imp_rat = piped_imp/(piped + piped_imp)
			local piped_imp_prop = piped_imp_rat

		//apply proportion that is improved to the responses that require crosswalking
		use `master', clear
		replace wash_water_imp_prop = `well_imp_prop' if w_source_drink_mapped == "well_cw"
		replace wash_water_imp_prop = `spring_imp_prop' if w_source_drink_mapped == "spring_cw"
		replace wash_water_imp_prop = `piped_imp_prop' if w_source_drink_mapped == "piped_cw"
		replace wash_water_piped = 1 - `piped_imp_prop' if w_source_drink_mapped == "piped_cw"
	}

// Improved and sewer sanitation indicator generation
	cap confirm variable t_type_mapped
	if !_rc {
		di as error "creating toilet indicator..."
		//create variable that indicates if the sanitation response is valid
		gen san = 0 if mi(t_type_mapped) | t_type_mapped == "other" | t_type_mapped == "unknown"
		replace san = 1 if san != 0

		//sewer connection/septic sanitation
		gen wash_sanitation_piped = 1 if t_type_mapped == "flush_imp" | t_type_mapped == "sewer" | t_type_mapped == "septic"
		replace wash_sanitation_piped = 0 if wash_sanitation_piped != 1 & san == 1

		//improved sanitation, excluding sewer connection
		gen wash_sanitation_imp_prop = 1 if t_type_mapped == "improved" | t_type_mapped == "latrine_imp"
		replace wash_sanitation_imp_prop = 0 if san == 1 & wash_sanitation_piped != 1 & wash_sanitation_imp_prop != 1
		tempfile master
		save `master', replace

		//snag crosswalking ratios from prepped tempfile
		use `sani_cw', clear

			//proportion of the ambiguous latrine string that is "improved"
			gen lat_imp_rat = latrine_imp/(latrine_unimp + latrine_imp)
			local latrine_imp_prop = lat_imp_rat

			//proportion of the ambiguous flush string that is "improved"
			gen flush_imp_rat = flush_imp/(flush_imp + flush_unimp)
			local flush_imp_prop = flush_imp_rat

		//apply proportion that is improved to the responses that require crosswalking
		use `master', clear
		replace wash_sanitation_imp_prop = `latrine_imp_prop' if t_type_mapped == "latrine_cw"
		replace wash_sanitation_piped = `flush_imp_prop' if t_type_mapped == "flush_cw"
		replace wash_sanitation_piped = 0 if t_type_mapped == "latrine_cw" & mi(wash_sanitation_piped)

		// shared sanitation check
		if `shared' == 1 {
			cap confirm variable shared_san
			if !_rc {
				replace wash_sanitation_imp_prop = 0 if shared_san == 1
				replace wash_sanitation_piped = 0 if shared_san == 1
				gen shared = 1 if shared_san == 1
				replace shared = 0 if shared_san == 0
			}
		}
	}


// household water treatment
cap confirm variable w_boil w_filter w_solar w_bleach
if !_rc {
	//no treatment category
	gen hwt = 0 if mi(w_boil) & mi(w_solar) & mi(w_filter) & mi(w_bleach)
	local survey = survey_name
	if "`survey'" == "UNICEF_MICS"{
		replace hwt = 1
		replace w_boil = 0 if w_boil != 1
		replace w_bleach = 0 if w_bleach != 1
		replace w_filter = 0 if w_filter != 1
		replace w_solar = 0 if w_solar != 1
	}
	replace hwt = 1 if hwt != 0
	gen wash_no_treat = 1 if w_boil == 0 & w_filter == 0 & w_solar == 0 & w_bleach == 0
	replace wash_no_treat = 0 if wash_no_treat != 1 & hwt == 1

	//boil/Filter category
	gen wash_filter_treat_prop = 1 if w_boil == 1 | w_filter == 1
	replace wash_filter_treat_prop = 0 if wash_filter_treat_prop != 1 & wash_no_treat == 0
}


// For surveys that don't ask about solar disinfection
cap confirm variable w_boil w_filter w_bleach
if !_rc {
	cap confirm variable wash_filter_treat_prop
	if _rc {
		//no treatment category
		gen hwt = 0 if mi(w_boil) & mi(w_filter) & mi(w_bleach)
		local survey = survey_name
	if "`survey'" == "UNICEF_MICS"{
		replace hwt = 1
		replace w_boil = 0 if w_boil != 1
		replace w_bleach = 0 if w_bleach != 1
		replace w_filter = 0 if w_filter != 1
	}
		replace hwt = 1 if hwt != 0
		gen wash_no_treat = 1 if w_boil == 0 & w_filter == 0 & w_bleach == 0
		replace wash_no_treat = 0 if wash_no_treat != 1 & hwt == 1

		//boil/Filter category
		gen wash_filter_treat_prop = 1 if w_boil == 1 | w_filter == 1
		replace wash_filter_treat_prop = 0 if wash_filter_treat_prop != 1 & wash_no_treat == 0
	}
}

// Generate vars with no observations for missing vars so the collapse won't break
local vars wash_water_imp piped_mod improved_san sewer handwashing air_hap any_hwt_piped any_hwt_imp any_hwt_unimp boil_filter_piped boil_filter_imp boil_filter_unimp
foreach var of local vars {
	cap confirm variable `var'
	if !_rc {
		cap assert mi(`var')
		if !_rc {
			drop `var'
		}
	}
}

// Adjust for family size
if survey_module == "HH" {
	cap confirm numeric variable hh_size pweight
	if !_rc {
		replace hh_size = 35 if hh_size > 35 & hh_size != .
		replace pweight = pweight * hh_size
	}
}	

// Clean and save
	local vars "cooking_fuel w_source_drink t_type t_type_mapped w_source_drink_mapped cooking_fuel_mapped water_std sanitation_std mins_ws mins_ws_zero shared_san shared_san_num_greater_ten shared_san_num w_source_other w_settle w_cloth w_bleach w_boil w_filter w_treat w_solar hw_soap1 hw_soap2 hw_soap3 age_year hw_station hw_water hw_soap water san hwt"
	foreach var of local vars {
		cap confirm variable `var'
		if !_rc {
			drop `var'
		}
	}
