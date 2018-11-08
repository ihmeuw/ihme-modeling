// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: USERNAME
// Date: DATE
// Description: Creates an n-code disability weight hierarchy, empirical
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
// THIS IS DONE FOR ONLY ONE COUNTRY

// prep stata
clear all
set more off

global prefix "FILEPATH"

// create locals to run on computer

local input_dir "FILEPATH"
local output_dir "FILEPATH" 
local code_dir "FILEPATH"
local input_dir_other "FILEPATH"
local dur_dir "FILEPATH"

// get shared functions

adopath + "FILEPATH"
adopath + "FILEPATH"
adopath + "FILEPATH"
adopath + "`code_dir'/ado"

// store the n-code disability weights for regressions later

insheet using "`input_dir_other'/FILEPATH.csv", comma names clear
egen dw = rowmean(draw*) // create a mean disability weight from the draws
rename n_code ncode
keep ncode dw

tempfile n_dw_map // create a file of the mean disability weight and ncode
save `n_dw_map', replace

levelsof ncode, local(n_codes) clean

foreach ncode of local n_codes {
	use `n_dw_map', clear
	keep if ncode == "`ncode'"
	local dw_`ncode' = dw[1] // create a local for each N-code's disability weight
}

// load the parameters needed for this code
hierarchy_params, prefix($prefix) repo(`code_dir') steps_dir(`input_dir') // it is a list of ncodes, their groups, and descriptions

// get the number of ncodes for use in sizing the matrix later on
local num_n = wordcount("$ncodes")

tempfile current_dataset

// load in the prepped dataset
use "FILEPATH.dta", clear

keep if inpatient == 1 | inpatient == .
save `current_dataset', replace
local hosp 1

// create locals for no-LT and all-LT information so that we can iteratively adjust them separately for each severity //  LONG-TERM = lt

local no_lt ${no_lt}
local no_lt_sev ${no_lt_sev}
local all_lt ${all_lt}
local all_lt_sev ${all_lt_sev}

while "`no_lt' `all_lt'" != " " {

	display "no_lt: `no_lt'"
	display "no_lt_sev: `no_lt'"
	display "no_lt_tot: `no_lt_tot'"
	display "all_lt: `all_lt'"
	display "all_lt_sev: `all_lt_sev'"
	display "all_lt_tot: `all_lt_tot'"

	use `current_dataset', clear

	// add recent additions to the 100% LT groups to a local (used to append on these DWs after regression model is done)

	local all_lt_tot `all_lt_tot' `all_lt'
	local no_lt_tot `no_lt_tot' `no_lt'

	// drop variables that previously resulted in negative DWs (or are a priori no-LT)
	local num_no_lt = wordcount("`no_lt'")
	forvalues x = 1/`num_no_lt' {
			local n = word("`no_lt'",`x')
			local sev = word("`no_lt_sev'",`x')
			if inlist(`sev',9,1) drop INJ_`n' 
		}

	local no_lt
	local no_lt_sev

	// set indicated N-code/hospital combinations to 100% LT
	local num_caps = wordcount("`all_lt'")

	forvalues x = 1/`num_caps' {
		local ncode = word("`all_lt'", `x')
		local sev = word("`all_lt_sev'", `x')
		cap confirm variable INJ_`ncode'
		if !_rc { // do ONLY IF the variable exists
			if inlist(`sev',9,1) {

				replace logit_dw = logit(1 - ((1 - invlogit(logit_dw)) / (1 - (INJ_`ncode')*(`dw_`ncode'')))) if INJ_`ncode' != 0
				replace logit_dw = 0.00001 if logit_dw == . & INJ_`ncode' != 0
				drop INJ_`ncode'
			}
		}
	}

	local all_lt
	local all_lt_sev

	// save the adjusted dataset
	save `current_dataset', replace

	// run regression to get predicted effects of comorbidities and injuries
	// fixed effects using all injuries, allowing interaction between age group, sex, and never_injured covariates to predict the disability weight
	// the random effects are by country and by id
	mixed logit_dw age_gr##sex##never_injured INJ* if (TIME <= 0 | TIME > 364) || iso3: || id:
	
	// Generate matrices to hold results
	clear mata
		mata: INJ = J(`num_n',1,"")
		foreach col in DW_O DW_S DW_T N {
			mata: `col' = J(`num_n', 1, .)
		}
	local c = 1
	// record resulting disability weight for each N-code from the mixed effects model
	foreach como of varlist INJ* {
			preserve
			keep if  `como' > 0
			summ `como'
			local n = `r(sum)'
			if `n' {
			// account for the probabilistic mapping that has partial values for dummy variable
			
			// predict the disability weight if that injury is present
				replace `como' = 1
				predict dw_obs
				replace dw_obs = invlogit(dw_obs)
			// predict the disability weight if that injury is not present
				replace `como' = 0	
				predict dw_s
				replace dw_s = invlogit(dw_s)
			
			// transform the disability weight to be between 0 and 1, with larger
			// disability weights because of injuries that cause more disability
				gen dw_t = (1 - ((1-dw_obs)/(1-dw_s)))

			// take the summary of the transformed disability weights
			// and then save the mean
				summ dw_t
				local mean_dw_tnoreplace = `r(mean)'
				summ dw_obs
				local mean_dw_o = `r(mean)'
				summ dw_s
				local mean_dw_s `r(mean)'
			
			// drop the INJ_ prefix from the N-code
			local short_como = subinstr("`como'","INJ_","",.)
			
			// set 0 LT n-codes for next iteration of regression based on which resulting
			// disability weights are negative
			
			if `mean_dw_tnoreplace' < 0 {
					local no_lt `no_lt' `short_como'
					local no_lt_sev `no_lt_sev' `hosp'
				}
			
			// cap the LT at the GBD disability weight
			
			if `mean_dw_tnoreplace' > `dw_`short_como'' {
					local all_lt `all_lt' `short_como'
					local all_lt_sev `all_lt_sev' `hosp'
				}
				mata: INJ[`c', 1] = "`short_como'"		
				mata: DW_T[`c', 1] = `mean_dw_tnoreplace'
				mata: DW_O[`c', 1] = `mean_dw_o'
				mata: DW_S[`c', 1] = `mean_dw_s'
				mata: N[`c', 1] = `n'
				local c = `c' + 1
		}
		else drop `como'
		restore
	}
	
	// keep only the relevant part of the matrix,
	// the part that was filled in by the N-codes above
	local c = `c' - 1
	foreach col in INJ DW_O DW_S DW_T N {
		mata: `col' = `col'[1::`c',1]
	}
}

// get results into dataset once there are no more adjustments to make
clear
getmata INJ DW_O DW_S DW_T N
assert DW_T != .

// keep the relevant variables
keep INJ N DW_T
rename(DW_T INJ) (dw_`hosp' ncode)

// ADD on a disability weight and N for N-codes that
// we dropped: no longterm and all-longterm

// merge on the N-codes that we dropped

merge 1:1 ncode using "`input_dir_other'/FILEPATH.dta", nogen
drop if regexm(ncode, "^GS")
replace N = N_1 if N == .
drop N_*
rename N N_`hosp'

// merge on N-codes for 100% long-term data

merge 1:1 ncode using `n_dw_map', nogen
if `hosp' == 0 {
		replace dw_`hosp' = dw if dw_`hosp' == . & subinword("`all_lt_tot'",ncode,"",.) != "`all_lt_tot'" & ///
		subinword("${hosp_only}",ncode,"",.) == "${hosp_only}"
	}
	else replace dw_`hosp' = dw if dw_`hosp' == . & subinword("`all_lt_tot'",ncode,"",.) != "`all_lt_tot'"
	drop dw
	
	** ** set DW = 0 for all no-LT n-codes
	replace dw_`hosp' = 0 if dw_`hosp' == . & subinword("`no_lt_tot'",ncode,"",.) != "`no_lt_tot'"
	
// save the results of this hospital's regression analysis
save "`output_dir'/FILEPATH.dta", replace

