// THIS PROGRAM GENERATES A "PERCENT TREATED" FOR INJURIES BASED ON THE HEALTHCARE ACCESS AND QUALITY COVARIATE

cap program drop get_pct_treated
program define get_pct_treated
	version 12
	syntax , prefix(string) code_dir(string) [allyears]	
	
// Set arbitrary minimum fraction treated
	local min_treat .1
	
// Get list of GBD locations and years
	adopath + "`code_dir'/ado"
// Get covariates functions
	adopath + "FILEPATH"
	
// Get list of desired years
	get_demographics_template, gbd_team("epi") gbd_round_id(4) clear
		keep location_ids
		rename location_ids location_id
		duplicates drop

		tempfile metadata
		save `metadata'

// Get covariate values and generate 1000 draws from normal distribution
	// based off of se and mean values using calc_se in .ado files
	get_covariate_estimates, covariate_id(1099) clear
		
		keep location_id year_id *value

		calc_se lower_value upper_value, newvar(se)
		rename mean_value mean
		drop *value

		// generate draws
		foreach i of numlist 0/999 {
			gen haqi_`i' = rnormal(mean, se)
		}

		drop mean

		tempfile covars
		save `covars'

	use "`metadata'", clear
		merge 1:m location_id using "`covars'", nogen

// develop scale

	// we are setting the max healthcare access and quality index value at 75 (100% treated) and scaling everything off of that
	// it covers most of western europe and the U.S. back to the 90's

	foreach i of numlist 0/999 {
		di "On loop: `i'"
		qui replace haqi_`i' = 75 if haqi_`i' > 75

		gsort - haqi_`i'
		local min_value = haqi_`i'[_N]
		local max_value = haqi_`i'[1]
		set type double

		qui gen pct_treated_`i' = `min_treat' + ( (1 - `min_treat') * (haqi_`i' - `min_value') / (`max_value' - `min_value') )
		
		drop haqi_`i'
	}

// format
	keep location_id year_id pct_treated*
	sort year_id location_id
	order location_id year_id pct_treated*
	
end
