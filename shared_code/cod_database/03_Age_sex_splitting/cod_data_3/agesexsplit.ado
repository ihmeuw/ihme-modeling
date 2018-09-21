** ***************************************************************************	**
** Purpose: ado-file to take in age group limits and weights and output 5-year death counts	
** Inputs: This ado-file should be called on a dataset with the following variables:			
**		age_start: lower age limit (min: 91 (infant deaths, 0-6 days, max: 80)				
**		age_end: upper age limit (min: 91 (infant deaths, 0-6 days, max: 80)				
**		cause: mapped to GBD cause hierarchy
**		sex: 1 = male, 2 = female, 9 = both or unknown								
**		deaths: number of deaths													
** 		pop: population by age group for that site/study/country (will be merged on)		
**		weights: age-specific death rates from pooled VR data (will be merged on)			
** ************************************************************************		**
		
// define the program so that Stata knows that it is a command
capture program drop agesexsplit
program agesexsplit

syntax, splitvar(string) splitfil(string) outdir(string) weightdir(string) mapfil(string) dataname(string) cause(string) wtsrc(string)

** ** ******************************************************* **
clear
set more off

quietly {

** ***********************************************************************************************************************
// pull out the country-years we are working with in the splitfil to make reshaping the pop file faster
	** start with the observations to be split for the current cause
	use if cause == "`cause'" using "`splitfil'", clear
	
	** files may or may not have a variable named "source," but if they do, we want to use it:
	local srce source
	keep iso3 location_id year `srce'
	
	** reduce to necessary variables and observations
	capture drop `srce'
	duplicates drop
	
	** save for later
	tempfile cys
	save `cys'

// prep popfil: format the population file to make it easier to merge onto the weights file
	use "$temp_dir/pop_prepped.dta", clear
	** below is new for subnational fix
	drop location_id

	** first keep only the country-years that we have in the file to be split so we don't have to waste time reshaping pop data we don't need anyway
	** merge on isopop rather than iso3 -- this will allow us to designate alternate populations for country-years where we don't have population 
	joinby iso3 year using `cys'

	** rename variables so that they are named pop_0, pop_1, pop_5, pop_10, ... instead of pop2, pop3, pop7, ...
	capture drop pop_tot
	capture drop envelope*
	** rename pop2 pop_0
	egen pop_0 = rowtotal(pop91-pop94)
	rename pop3 pop_1
	forvalues i = 7/22 {
		local j = (`i' - 6) * 5
		rename pop`i' pop_`j'
	}
	renpfix pop_ pop
		
	** make age long (sex is already long)
	reshape long pop, i(iso3 location_id year sex /* country pop_source*/) j(age)
	
	
	** save
	tempfile pop
	save `pop', replace

** ***********************************************************************************************************************
// load in the weights: look in the directory specified in `weightdir' for the weight source specified in `wtsrc' 
// (should generally be "ICD10_and_9_weights") and the cause specified in `cause'
	if "`cause'" == "_u" {
		use "`weightdir'/`wtsrc'_age_weight__gc.dta", clear
	}
	else {
		use "`weightdir'/`wtsrc'_age_weight_`cause'.dta", clear
	}
	rename acause cause
	replace age = 91 if age==0
	replace age = 93 if age>0.009 & age<0.011
	replace age = 94 if age>0.09 & age<0.11

	** merge on population so we can calculate WN, which is just weight*pop for each age and sex
	merge 1:m sex age using `pop', keep(match) nogenerate
	
	** now calculate WN
	generate WN = weight*pop
		
	** make age wide
	reshape wide WN weight pop, i(iso3 location_id year sex cause) j(age)
	renpfix WN WN_
	renpfix weight weight_
	renpfix pop pop_
	capture rename pop__source pop_source
	
	** make sex wide
	reshape wide WN* weight* pop*, i(iso3 location_id year cause) j(sex)
	
	** fix variable names for clarity
	foreach a of numlist 1 5(5)80 91 93 94 {
		rename weight_`a'1 weight_`a'_1
		rename weight_`a'2 weight_`a'_2
		rename pop_`a'1 pop_`a'_1
		rename pop_`a'2 pop_`a'_2
		rename WN_`a'1 WN_`a'_1
		rename WN_`a'2 WN_`a'_2
	}	
	
	** save so we can merge onto the dataset to be split
	tempfile wts
	save `wts', replace

** ***********************************************************************************************************************
// now open the dataset to be split for the current cause (use the cause specified in `cause' and the file specified by `splitfil')
	use if cause == "`cause'" using "`splitfil'", clear
		
		// merge on the `wts' tempfile, which contains the weights, population, and WN (=weight*pop)
		// Here, we need to merge on isopop rather than iso3 in instances in which we're using an alternate country's population 
		merge m:1 iso3 location_id year cause using `wts'
		drop if _merge == 2

	** In some cases, we won't have the population numbers we need. Maybe we're missing population for the entire country-year,  or maybe we are just missing infant population numbers for age groups that need to be split into neonatal age groups. 
	** For those cases, mark them here so we can reidentify them later when we go to combine the split data with the data that didn'tneed to be split.  We'll want to retain their "bad" frmats.
	foreach i of numlist 1 5(5)80 91 93 94 {
		gen nopop_`i' = 0
		replace nopop_`i' = 1 if _m == 1 | pop_`i'_1 == .
	}

	drop _merge

** ***********************************************************************************************************************
// Calculate K
	// calculate K in two steps: first the denominator(sum of WN for each constituent age-sex group in the lumped group)...
	generate Kdenom = .
	aorder
	order WN_91* WN_93* WN_94*, before(WN_1_1)
	order nopop_91 nopop_93 nopop_94, before(nopop_1)
	
	// first calculate the denominator. to do this, we'll have to loop through each age_start/_end combination...
	// rename age groups so data is relatively neat (put infant age groups before adult age groups)
	replace age_start = 4 if age_start == 1
	replace age_start = 1 if age_start == 91
	replace age_start = 2 if age_start == 93
	replace age_start = 3 if age_start == 94
	replace age_end = 4 if age_end == 1
	replace age_end = 1 if age_end == 91
	replace age_end = 2 if age_end == 93
	replace age_end = 3 if age_end == 94
	renpfix WN_1_ WN_4_
	renpfix WN_91_ WN_1_
	renpfix WN_93_ WN_2_
	renpfix WN_94_ WN_3_
	rename nopop_1 nopop_4
	rename nopop_91 nopop_1
	rename nopop_93 nopop_2
	rename nopop_94 nopop_3
	
	foreach i of numlist 1/4 5(5) 80 {
		foreach j of numlist 1/4 5(5) 80 {
			if `i' <= `j' {
				// calculate Kdenom
				egen tmp = rowtotal(WN_`i'_1 - WN_`j'_2)
				replace Kdenom = tmp if age_start == `i' & age_end == `j'
				drop tmp
				
				// mark as missing for ages without the necessary population information
				egen tmp = rowtotal(nopop_`i' - nopop_`j')
				replace Kdenom = . if tmp > 0
				drop tmp
			}
		}
	}
				
	// now we can calculate K:
	generate K = `splitvar' / Kdenom

** ***********************************************************************************************************************
// and finally, calculate D_a,s = K * W_a,s * N_a,s
	// to do this, we'll again loop through the age_start/_end combos
		// first generate the variables we need to fill in
			foreach i of numlist 1/4 5(5)80 {	// loop over ages
				foreach s of numlist 1 2 {	// loop over sexes
					generate `splitvar'_`i'_`s' = .
				}
			}
		// this loop will take care of ALL age-sex combos!
			foreach i of numlist 1/4 5(5)80 {	// loop over age_start possibilities
				foreach j of numlist 1/4 5(5)80 {	// loop over age_end possibilities
					foreach k of numlist 1/4 5(5)80 {	// loop over age possibilities
						foreach s of numlist 1 2 {	// loop over sexes
							if (`i' <= `j') & (`k' >= `i' & `k' <= `j') {
								replace `splitvar'_`k'_`s' = (K*WN_`k'_`s') if age_start == `i' & age_end == `j'
							}
						}						
					}
				}
			}	
	
** ***********************************************************************************************************************
	// reshape sex to long rather than wide, for consistency with the other split formats
	rename sex sex_orig
	reshape long deaths_1 deaths_2 deaths_3 deaths_4 deaths_5 deaths_10 deaths_15 deaths_20 deaths_25 ///
		deaths_30 deaths_35 deaths_40 deaths_45 deaths_50 deaths_55 deaths_60 deaths_65 deaths_70 ///
		deaths_75 deaths_80, i(iso3 location_id year *national subdiv *frmat* cause_orig age_start NID) j(sex) string
	replace sex = regexs(0) if regexm(sex, "([0-9])")
	destring sex, replace

	// fix the names we messed with earlier
	rename `splitvar'_1 `splitvar'_91
	rename `splitvar'_2 `splitvar'_93
	rename `splitvar'_3 `splitvar'_94
	rename `splitvar'_4 `splitvar'_1
	
	// get rid of unnecessary variables
	drop Kdenom K WN_* weight_* pop_* age* 
	capture drop isopop
	
** ***********************************************************************************************************************
	// save with the j specified in `dataname' for the current cause
	di in red "Saving `dataname'_agesexsplit_`cause'.dta"
	save "`outdir'/`dataname'_agesexsplit_`cause'.dta", replace
}
capture drop isopop

end
