// Use DisMod input for age distribution and country-level paf estimates from
// 00_studylevel_pcv.do for final pneumo paf estimates //
// hib_final_paf.do must be run prior to this file //

// set up //
else global j "J:"
set more off
cap log close
clear all
set matsize 8000
set maxvar 10000
//set maxvar 32000

local date = c(current_date)

do "FILEPATH/get_covariate_estimates.ado"
do "FILEPATH/get_location_metadata.ado"


// Get IHME location metadata
get_location_metadata, location_set_id(9) clear
tempfile locations
save `locations'

// Get vaccine coverage estimates, these may be updated soon
get_covariate_estimates, covariate_name_short(Hib3_coverage_prop) clear
drop age_group_id
drop sex_id
gen fcovmean = mean_value
gen fcovsd = (upper_value-lower_value)/2/1.96
gen hibmean = mean_value

tempfile hib_cov
save `hib_cov'

local vi .8   

	use "FILEPATH\vaccine type coverage.dta", clear
	keep if vtype == "PCV7"
	replace covmean = covmean / 100
	replace covupper = covupper / 100
	replace covlower = covlower / 100
	
	tempfile vtype
	save `vtype'		
	
	use location_id year_id paf_mean paf_sd fcovmean fcovsd optvi using "FILEPATH\hib_RCT_studypaf.dta", clear
	merge m:1 location_id using `locations'
	drop _m
	rename paf_mean hibmean
	rename fcovmean hib_cov
	rename fcovsd hib_covsd

	rename optvi hibve
	gen hibvese = .0001

	tempfile hibve
	save `hibve'

use "FILEPATH/pneumo_vaccine_covs.dta", clear
save `vtype', replace

	import delimited "FILEPATH/age_mapping.csv", clear
	tempfile map
	save `map'
	
	use "FILEPATH/reshaped_draws.dta", clear
	merge 1:m id using `map', keep(3) nogen

	egen lower_95 = rowpctile(draw*), p(5)
	egen median = rowmedian(draw*)
	egen upper_95 = rowpctile(draw*), p(95)
	egen adve = rowmean(draw*)
	label variable adve "Adjusted VE for PCV"
	
	tempfile madve
	save `madve'
	levelsof age_group_id, local(ages) c
	saveold "FILEPATH/pcv_paf_byage.dta", replace
		
	
******** Get the file of PCV pafs by country year. It includes PCV coverage, PAFs ********		
	use `vtype', clear
	
// Formula to combine all parameters: gen finve = (adve / (1 - hib_mean * hib_cov * hibve))* (1 - fcovmean * `vi') / (1 - (adve / (1 - hib_mean* hib_cov * hibve)) * fcovmean * `vi')
			
	label variable hib_cov "Country hib coverage (from Margot)"
	label variable hibve "Country hib coverage efficacy (assumed)"
	label variable hib_mean "Country hib VE after adjustment only RCT"
	label variable vcov "PCV vaccine coverage"
	label variable vtcov "PCV7 Vaccine type coverage"
	label variable fcovmean "Vaccine coverage * vaccine type"

	gen mrg = 1				
	gen all_id = _n
	
	tempfile main
	save `main'

// Import Hib results for age_group_id 1 //

	use location_id year_id age draw* using "FILEPATH/hib_paf_draws_2.dta", clear
	keep if age== "1"
	forval i = 1/1000 {
		rename draw`i' hib_paf_`i'
	}
	tempfile hib_draws
	save `hib_draws'
				
qui		foreach ag of local ages { 
			di in red "`ag' " _c
			local cnt = `cnt' + 1 
			use `madve', clear
			keep if age_group_id == `ag'
			gen mrg = 1
			tempfile madveuse
			save `madveuse'
			use `main', clear
			
			cap gen mrg = 1
			merge m:1 mrg using `madveuse', nogen
			sort all_id
			
// Prep values for rnormal() //	
			gen hib_lower=hib_cov-hib_covsd*1.96
			gen hib_upper = hib_cov + hib_covsd*1.96
						
			replace hib_cov = logit(hib_cov)
			replace hib_covsd = (logit(hib_upper)-logit(hib_lower))/1.96/2
			
			gen fcov_lower = fcovmean-fcovsd*1.96
			gen fcov_upper = fcovmean + fcovsd*1.96
			
			replace fcovmean = logit(fcovmean)
			replace fcovsd = (logit(fcov_upper)-logit(fcov_lower))/1.96/2

			tempfile master
			save `master'
			
			use `hib_draws', clear
			merge m:m location_id year_id using `master', nogen
			
			keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2016)

// Calculate PAFs //
// No Hib PAF above 5 years //
			forval j = 1/1000{
				replace hib_paf_`j' = 0 if age_group_id <=3
				replace hib_paf_`j' = 0 if age_group_id >5
				replace hib_paf_`j' = 0 if hib_paf_`j' < 0
				replace hib_paf_`j' = 1 if hib_paf_`j' > 1

				gen hib_cov_`j' = invlogit(rnormal(hib_cov, hib_covsd))
				replace hib_cov_`j' = 0 if hib_cov_`j' == .
				gen fcov_`j' = invlogit(rnormal(fcovmean, fcovsd))
				replace fcov_`j' = 0 if fcov_`j' == .
				
				gen paf_`j' = (draw`j' / (1 - hib_paf_`j' * hib_cov_`j' * `vi')) * (1 - fcov_`j' * `vi') / (1 - (draw`j' / (1 - hib_paf_`j' * hib_cov_`j' * `vi')) * fcov_`j' * `vi')
			}
			drop draw1-draw1000
			//drop optv1-optv1000
			drop hib_paf_* hib_cov_* fcov_* 
			
			tempfile main2
			save `main2', replace
			save "FILEPATH/paf_`ag'_`date'.dta", replace

			order location_id year_id age_group_id
			if `cnt'== 1 {
						tempfile byage
						save `byage'
			}
			else {
				append using `byage'
				save `byage', replace 
			}
}


cap drop optv1-optv1000
cap drop v1-v1000

saveold "FILEPATH/pcv_paf_draws.dta", replace 

