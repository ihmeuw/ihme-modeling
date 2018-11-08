// Purpose:Custom_code to prep WaSH and HAP indicators for tabulation
if c(os) == "Unix" {
	local j "FILEPATH"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	local j "FILEPATH"
}


// HAP toggle
local hh_size_adj = 1
local shared = 0 



// Household air pollution indicator
	local fuels "gas biomass coal crop kerosene wood dung other"
	cap confirm variable cooking_fuel_mapped
	if !_rc {
		gen air_hap = .
		replace air_hap = 0 if cooking_fuel_mapped == "gas" | cooking_fuel_mapped == "kerosene"
		replace air_hap = 1 if cooking_fuel_mapped == "biomass" | cooking_fuel_mapped == "coal" | cooking_fuel_mapped == "crop" | cooking_fuel_mapped == "wood" | cooking_fuel_mapped == "dung"
		// foreach fuel of local fuels {
			// gen hap_`fuel' = .
			// replace hap_`fuel' = 1 if cooking_fuel_mapped == "`fuel'"
			// replace hap_`fuel' = 0 if cooking_fuel_mapped != "`fuel'" & cooking_fuel_mapped != ""
		// }
	}

// Generate vars with no observations for missing vars so NAME's collapse won't break
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
/*
// Tag if has no indicators of interest
cap confirm variable wash_water_imp
if _rc {
	cap confirm variable improved_san
	if _rc {
		cap confirm variable hap_expose
		if _rc {
			gen empty = 1
		}
	}
}
*/
// Clean and save
	local vars "cooking_fuel w_source_drink t_type t_type_mapped w_source_drink_mapped cooking_fuel_mapped water_std sanitation_std mins_ws mins_ws_zero shared_san shared_san_num_greater_ten shared_san_num w_source_other w_settle w_cloth w_bleach w_boil w_filter w_treat w_solar hw_soap1 hw_soap2 hw_soap3 age_year hw_station hw_water hw_soap water san hwt"
	foreach var of local vars {
		cap confirm variable `var'
		if !_rc {
			drop `var'
		}
	}
