// Calculate study-level attributable fractions for Pneumococcal pneumonia.
// These outputs are then used in DisMod to calculate age-specific PAFs
// the meta-analytic effect size result is then adjusted for national-level
// values to estimate a final pneumo paf

// Set up files and functions //
clear all
set more off
cap log close
set maxvar 15000
set seed 2038947

do "FILEPATH/get_covariate_estimates.ado"
do "FILEPATH/get_location_metadata.ado"

// run Hib first !! //
local pat  pcv
local file Study-Level_PAFs

// This is an adjustment to account for imperfect vaccine effectiveness
// The value comes from Lucero et al. 2009 //
local vi .8   

// Get vaccine coverage estimates //
get_covariate_estimates, covariate_name_short(Hib3_coverage_prop) clear
drop age_group_id
gen fcovmean = mean_value
gen fcovsd = (upper_value-lower_value)/2/1.96
tempfile hib_cov
save `hib_cov'

get_covariate_estimates, covariate_name_short(PCV3_coverage_prop) clear
drop age_group_id
gen pcv_mean = mean_value
gen pcv_sd = (upper_value-lower_value)/2/1.96
tempfile pcv_cov
save `pcv_cov'

// Get IHME location metadata //
get_location_metadata, location_set_id(9) clear
tempfile locations
save `locations'


// PCV serotype coverage by type (7,10,11,13) and Super Region //
// This is from Johnson et al. 2010 //
	use "FILEPATH/vaccine_type_cov.dta", clear
	keep if vtype == "PCV7"
	replace covmean = covmean / 100
	replace covupper = covupper / 100
	replace covlower = covlower / 100
	tempfile vtype
	save `vtype'		
	
	// Output from PAF calculation for Hib //
	// fcovmean is estimated vaccine coverage //
	// finve is estimated PAF (not from draws) //
	// optvi is the optimal vaccine efficacy (0.8) //
	
// import hib study-level paf estimates //
	use location_id year_id finve fcovmean fcovsd optvi using "FILEPATH/hib_RCT_studypaf.dta", replace
	rename finve hib_mean
	rename fcovmean hib_cov
	rename fcovsd hib_covsd
	gen hib_sd = .05
	rename optvi hibve
	gen hibvese = .0001
	tempfile hibve
	save `hibve'
	
// Combine all the vaccine coverage, vaccine-type coverage, and Hib PAF output into a single file //
	use `pcv_cov', clear
	merge m:1 location_id using `locations', keep(3) nogen
	gen region = "Africa" if regexm(region_name, "Africa")
	replace region = "Asia" if regexm(region_name, "Asia")
	replace region = "Europe" if regexm(region_name, "Europe")
	replace region = "LAC" if regexm(region_name, "Latin") |  regexm(region_name, "aribbean")
	replace region = "North America" if regexm(region_name, "North America")
	replace region = "Oceania" if regexm(region_name, "Oceania")
	tab region_name if region == ""
	replace region = "North America" if regexm(region_name, "Australasia")

	merge 1:1 location_id year_id using `hibve', keep(1 3) nogen
	
	merge m:1 region using `vtype', keep(3) nogen
	gen vcov = pcv_mean
	rename covmean vtcov
	gen vcovsd = pcv_sd
	gen vtcovsd = (covupper - covlower) / (2*1.96)
	
	gen fcovmean = vcov * vtcov
// little equation for combining stds //
	gen fcovsd = sqrt(vtcovsd^2 * vcov^2 + vtcov^2 * vcovsd^2 + vcovsd^2 * vtcovsd^2)
	save `vtype', replace

// Import the PCV data from the literature review //
	import delimited "FILEPATH/lri_pneumo_data_2016.csv", clear
	drop if status == "EXCLUDE" // some studies have been identified for exclusion
	gen author = first + "_" + iso3 + string(studyyear_end)
	gen study = source
	gen Study_type = studytype
	gen ve = vaccineefficacy
	gen vel = lower95
	gen veu = upper95
	gen cov = vaccineserotype /100
	gen covl = lower_serotype /100
	gen covu = upper_serotype /100
	gen vi = vevtinvasivedisease
	gen vil = lower_vtinvasive
	gen viu = upper_vtinvasive
	
// Merge to vaccine type coverage //
	merge m:1 region vtype using "FILEPATH/vaccine_type_cov.dta", keep(3) nogen
	replace cov = covmean/100 if cov == .
	replace covl = covlower/100 if covl == .
	replace covu = covupper/100 if covu == .

	replace iso3 = "ARG" if iso3 == "Latin America"
	gen year_id = round((studyyear_start+ studyyear_end)/2 +.1,1)
	merge m:1 location_id year_id using `vtype', keep(1 3)
	
	replace vi = veallinasivedisease if veallinasivedisease != . & vi== .
	replace vil = lower_invasive if veallinasivedisease != . & vil==.
	replace viu = upper_invasive if veallinasivedisease != . & viu==.
	
	replace cov = 1 if veallinasivedisease != . & vevtinvasivedisease==.
	replace covl = 1 if veallinasivedisease != . & vevtinvasivedisease==.
	replace covu = 1 if veallinasivedisease != . & vevtinvasivedisease==.
	
// convert to ln //	
	foreach var of varlist ve vi {
		replace `var' = 99 if `var' == 100
		gen lnrr_`var' = ln(1-(`var'/100))
	}
	
	gen lnrr_veu= ln(1-(vel/100) + .005)
	gen lnrr_vel= ln(1-(veu/100) + .005)
	gen lnrr_viu= ln(1-(vil/100) + .005)
	gen lnrr_vil= ln(1-(viu/100) + .005)
	gen lnrr_vise = (lnrr_viu-lnrr_vil)/2/1.96
	
	replace vi = vi/100
	gen vi_se = (viu-vil)/2/1.96/100
	
	foreach p in ve {
		gen lnrr_`p'se = (lnrr_`p'u - lnrr_`p'l)/(2*1.96)
	}
	gen covse = (covu - covl)/(2*1.96)
		
	drop if vi == .
	gen dummy = 1
	tempfile data
	save `data'

/// Adjust the vaccine efficacy against invasive disease using Bonten et al 2015 //
/// ratio of VE invasive to VE pneumococcal pneumonia //
/// Use a uniform distribution from 0.3 to 1 ///
	clear
	set obs 1
	gen dummy = 1
	forval j = 1/1000 {
		gen uniform_adj_`j' = runiform(0.3, 1)
	}
	merge 1:m dummy using `data', nogen

/// Run caculation ///

	keep if lnrr_vese + lnrr_vise + covse != .
	keep if year_id >= 1980
	drop if vcov == .

	forval j = 1/1000 {
		gen draw_vi_`j' = 1-exp(rnormal(lnrr_vi, lnrr_vise))
		gen draw_ve_`j' = (1-exp(rnormal(lnrr_ve, lnrr_vese)))
		gen draw_cov_`j' = rnormal(cov, covse)
		gen draw_hib_`j' = rnormal(hib_mean, hib_sd)
		gen draw_hibve_`j' = rnormal(hibve, hibvese)
		gen uni_adj_`j' = draw_ve_`j' * (1 - (draw_hib_`j' * draw_hibve_`j')) / draw_cov_`j' / draw_vi_`j' / uniform_adj_`j'
	}
	foreach x of varlist uni_adj* {
		replace `x' = 0.995 if `x' >= 1
		replace `x' = ln(1-`x')
	}

	egen mean_uni = rowmean(uni_adj_*)
	egen std_uni = rowsd(uni_adj_*)
	
	cap drop cov_adj* draw_ve* draw_cov* draw_hib*  uni_adj* ratio* draw_vi* //average_*
	
	metan mean_uni std_uni, random label(namevar=author) title("Base S pneumoniae PAF") eform

	gen adj_lnrr = mean_uni
	gen adj_lnrrse = std_uni
	gen adj_adve = 1-exp(adj_lnrr)
	gen vi_adj = 1-exp(adj_lnrr)

	cap drop draw_cov*
	cap drop draw_hib*
	cap drop ratio_*
	
// Prepare for DisMod (sub/small functions) //
	gen time_lower = studyyear_start
	gen time_upper = studyyear_end
	gen age_lower = age_start
	gen age_upper = age_end
	gen super = "none"
	encode source,gen(sourceid)
	gen subreg = "none"
	gen integrand = "incidence"
// Run some draws to convert to normal space //
	forval j = 1/1000{
		gen draw_meas_`j' = exp(rnormal(adj_lnrr, adj_lnrrse))
	}
	egen meas_value = rowmean(draw_meas_*)
	egen meas_stdev = rowsd(draw_meas_*)
	drop draw_*

	gen x_sex = 0
	gen x_ones = 1
	gen x_befaft = 0
	gen x_ccorcohort = 0
	replace x_befaft = 1 if regexm(studytype,"fore")
	replace x_ccorcohort = 1 if regexm(studytype,"ontrol") | regexm(studytype,"ohort")
	drop if meas_value == .
	cap drop subreg
	cap drop region
	cap drop super

	drop _m
	merge m:1 location_id using `locations', keepusing(region_name super_region_name) keep(1 3)
	gen subreg = ihme_loc_id
	rename region_name region
	rename super_region_name super
	
	foreach var of varlist subreg region super {
		cap replace `var' = subinstr(`var'," ","_",.)
		cap replace `var' = subinstr(`var'," ","_",.)
		cap replace `var' = subinstr(`var'," ","_",.)
		cap replace `var' = subinstr(`var',"__","_",.)
	}

/// Save sheet for DisMod to estimate age-distribution of pafs ///
	outsheet source iso3 author studytype time_* age_* super region sourceid subreg integrand meas_value meas_stdev x_* ///
		using "FILEPATH/pcv_fordismod_regression.csv", comma replace

// Now create a file with the adjusted, unadjusted final, non-age specific PAF estimates at the country/year level (adjusted for coverage, vaccine type, and Hib PAF) ///
	metan adj_lnrr adj_lnrrse, random nograph
	local par_adj `r(ES)'
	local par_al `r(ci_low)'
	local par_au `r(ci_high)'

	use location_id year_id location_name fcovmean fcovsd vcov* vtcov* hib* using `vtype' , clear
	gen adve_adj = 1-exp(`par_adj')
	gen adve_adjse = adve_adj*(`par_au'-`par_al')/2/1.96
	gen optvi = `vi'
// Final PAF //
	gen finve_adj = (adve_adj / (1 - hib_mean * hib_cov * hibve))* (1 - fcovmean * `vi') / (1 - (adve_adj / (1 - hib_mean* hib_cov * hibve)) * fcovmean * `vi')
// Label variables, output //	
	label variable hib_cov "Country hib coverage (from Margot)"
	label variable hib_covsd "Country hib coverage SD(calculated from Margot)"
	label variable hibve "Country hib coverage effectiveness (assumed `vi')"
	label variable hib_mean "Country hib PAF after adjustment only RCT"
	label variable hibvese "Country hib coverage effectiveness (assumed)"
	label variable optvi "Current country PCV vaccination effectiveness (`vi')"
	label variable finve "Final PCV PAF for the country considering all parameters"
	label variable vcov "PCV vaccine coverage"
	label variable vtcov "PCV7 Vaccine type coverage"
	label variable fcovmean "Vaccine coverage * vaccine type"
	label variable finve_adj "Final PCV PAF adjusted by Bonten Study"
	
save "FILEPATH/pcv_studypaf_uniform.dta", replace

// Done!! // 
