** This file applies the recall bias correction estimated using all women's DHS sibling histories to all data sources
**
** IHME retains the copyright

** ************************************************************
** Load data
** ************************************************************
set more off
local date = c(current_date)
pause on

// Set options
local use_current = 1 // Set to 1 if you want to use the current regression estimates for this run; otherwise, it'll use the old estimates
local comp_date = "" // Set this to the date of the Regression Bias file that you want to use, if use_current == 0

cap log close

adopath + "FILEPATH/shared/functions" // Add get_locations function

log using "strLogFile", replace

** get location information 
get_locations
tempfile codes
save `codes', replace

** Read in model estimates from separate survey series and append them together
	// read in model estimates from DHS surveys
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "DHS"
	tempfile dhs
	save `dhs', replace

	// read in female respondent surveys from CDC RHS
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "CDC-RHS"
	append using `dhs'
	save `dhs', replace

	// read in female respondent surveys from IFHS
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "IFHS"
	append using `dhs'
	save `dhs', replace

	// read in female respondent surveys from PAPFAM
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "PAPFAM"
	append using `dhs'
	save `dhs', replace

	// read in male and female respondent surveys from GC13
	use "strSurveyDirectory/fullmodel_svy_census0.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "GC13"
	tempfile gc13
	save `gc13', replace

	use "strSurveyDirectory/fullmodel_svy_census1.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "GC13"
	append using `gc13'
	append using `dhs'
	save `dhs', replace

	// read in male and female respondent ZMB sexual behavior survey
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "sexual_behavior_zmb"
	replace svy = "ZMB_2000"
	append using `dhs'
	save `dhs', replace

	// read in male and female respondent IRQ mortality survey
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "ims_irq"
	replace svy = "IRQ_2011"
	append using `dhs'
	save `dhs', replace

	// read in female respondent surveys from LAO and BTN MICS survey
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "MICS"
	append using `dhs'
	save `dhs', replace

	// read in ZAF Subnational DHS surveys
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "dhs_zaf"
	append using `dhs'
	save `dhs', replace

	// read in BGD MHS surveys 
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "mhs_bgd"
	append using `dhs'
	save `dhs', replace

	// read in BRA Subnational DHS surveys
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "dhs_bra"
	append using `dhs'
	save `dhs', replace

	// read in Peru non-standard DHS surveys from 2013 and 2014
	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "dhs_per"
	append using `dhs'
	save `dhs', replace

	use "strSurveyDirectory/fullmodel_svy.dta", clear
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "dhs_per"
	append using `dhs'
	save `dhs', replace

	// read in Kenya subnational DHS from 2014
	use "strSurveyDirectory/fullmodel_svy.dta"
	keep female svy period sib45q15
	rename sib45q15 q45q15
	gen deaths_source = "dhs_ken"
	append using `dhs'
	save `dhs', replace


** ************************************************************
** data processing
** ************************************************************
// Save the compiled input dataset
save "strResultsDirectory/compiled_data_`date'.dta", replace

// assign year values to the periods for which the code was run 
gen year = substr(svy,-4,.)
destring year, replace
replace year = year - 1 if svy != "IRQ_2007"

replace year = (year-period) + 1.5 // So that the first period starts in 2013.5 if the survey was in 2014

// generate ihme_loc_id identifier
split svy, parse("_")
rename svy1 ihme_loc_id
replace ihme_loc_id = ihme_loc_id + "_" + svy2 if svy3 != "" & regexm(svy, "KEN")
drop svy2 svy3
replace ihme_loc_id = substr(ihme_loc_id,1,2) if substr(ihme_loc_id,1,2) == "AP" | substr(ihme_loc_id,1,2) == "UP"
replace ihme_loc_id = substr(ihme_loc_id,1,3) + "_" + substr(ihme_loc_id,4,.) if length(ihme_loc_id) > 3 & regexm(ihme, "KEN") != 1 // Add underscores if it's a subnational (except Kenya which already have underscores)
replace ihme_loc_id = "Pemba" if regexm(ihme_loc_id,"Pem")
replace ihme_loc_id = "Bohol" if regexm(ihme_loc_id,"Boh")

// generate sex variable
gen sex = "female" if female == 1
replace sex = "male" if sex == ""

tempfile results
save `results', replace

** drop some locations and add in the GBD super-region
keep if ihme_loc_id != "AP" & ihme_loc_id != "UP" & ihme_loc_id != "Pemba" & ihme_loc_id != "Bohol" & deaths_source != "sexual_behavior_zmb"

merge m:1 ihme_loc_id using `codes', keepusing(super_region_name)
drop if _merge == 2
drop _merge
tab super_region_name

drop if period ==.
sort ihme_loc_id year sex svy
by ihme_loc_id year sex: gen dup = _n

*** getting the pairs ***
tempfile alldata paired
keep svy q year ihme_loc_id sex
gen suryear = substr(svy,-4,.)
save `alldata', replace
levelsof svy, local(svys)

foreach v of local svys {
    use `alldata',clear
    keep if svy=="`v'"
	rename  svy basesvy
	rename q45q15 base45q15
	rename suryear base_suryear
	merge 1:m ihme_loc_id year sex using `alldata'
	keep if _merge==3
	drop _merge
	rename svy compsvy
	rename q45q15 comp45q15
	rename suryear comp_suryear
	keep if base_suryear>comp_suryear
	cap append using `paired'
	save `paired',replace
}

use `paired',clear
gen diff= comp45q15-base45q15
destring base_suryear comp_suryear,force replace
gen yeardiff=base_suryear - comp_suryear

// generate the log difference
gen lnbase45q15=ln(base45q15)
gen lncomp45q15=ln(comp45q15)
gen lndiff=lncomp45q15-lnbase45q15

// Output the dataset before running the regression
save "strResultsDirectory/inputs_paired_`date'.dta", replace

// recal bias regression
// non-logged model
di in red "This is the non-logged model, with no constant, for males"
reg diff yeardiff if sex=="male",noc
matrix b_male_diff = e(b)
matrix b_male_diff = b_male_diff[1,1]
matrix v_male_diff = e(V)
matrix se = vecdiag(v_male_diff)
matrix se = se[1,1]
matrix se = (sqrt(se[1,1]))
matrix lb_male_diff = b_male_diff - 1.96 * se
matrix ub_male_diff = b_male_diff + 1.96 * se

di in red "This is the non-logged model, with no constant, for females"
reg diff yeardiff if sex=="female",noc
matrix b_female_diff = e(b)
matrix b_female_diff = b_female_diff[1,1]
matrix v_female_diff = e(V)
matrix se = vecdiag(v_female_diff)
matrix se = se[1,1]
matrix se = (sqrt(se[1,1]))
matrix lb_female_diff = b_female_diff - 1.96 * se
matrix ub_female_diff = b_female_diff + 1.96 * se

// apply recall bias correction to estimates
use `results', clear

// recall bias for non-logged model
gen adj45q15 = q45q15 
gen adj45q15_lower = q45q15
gen adj45q15_upper = q45q15

	foreach sex in "male" "female" {
		if `use_current' == 1 {
			local mean_est = b_`sex'_diff[1,1]
			local lower_est = lb_`sex'_diff[1,1]
			local upper_est = ub_`sex'_diff[1,1]
		}
		else {
			preserve
			use "strCoefficientDirectory/allcoefficients_`comp_date'.dta" if sex == "`sex'", clear
			qui {
				levelsof yeardiff if coefficient == "b", local(mean_est)
				levelsof yeardiff if coefficient == "lb", local(lower_est)
				levelsof yeardiff if coefficient == "ub", local(upper_est)
			}
			restore
		}
		
		forvalues i = 1/15 {
			replace adj45q15 = adj45q15 + `mean_est' * `i' if period == `i' & sex == "`sex'"
			replace adj45q15_lower = adj45q15_lower + `lower_est' * `i' if period == `i' & sex == "`sex'"
			replace adj45q15_upper = adj45q15_upper + `upper_est' * `i' if period == `i' & sex == "`sex'"
		}
		replace adj45q15_upper = 1 if adj45q15_upper > 1 & adj45q15_upper != .
		replace adj45q15 = 1 if adj45q15 > 1 & adj45q15 != .
		replace adj45q15_lower = 1 if adj45q15_lower > 1 & adj45q15_lower != .
	}

tempfile adj_results
save `adj_results', replace

// generate a sourcedate variable
generate source_date = substr(svy, -4, .)

// save a file (include svy variable so that each observation is unique)
saveold "strResultsDirectory/EST_GLOBAL_SIB_45q15s.dta", replace  

** **************************************************
// SAVE COEFFICIENT MATRICES FROM RECALL BIAS REGRESSION
** **************************************************
	// female coefficients from non-logged model
	clear
	local date = c(current_date)
	matrix colnames b_female_diff = yeardiff 
	svmat b_female_diff, names(col)
	gen sex = "female"
	gen coefficient = "b"
	gen regression = "diff"
	tempfile female_coeffs_diff
	save `female_coeffs_diff'

	clear
	matrix colnames lb_female_diff = yeardiff 
	svmat lb_female_diff, names(col)
	gen sex = "female"
	gen coefficient = "lb"
	gen regression = "diff"
	tempfile female_lbcoeffs_diff
	save `female_lbcoeffs_diff'

	clear
	matrix colnames ub_female_diff = yeardiff 
	svmat ub_female_diff, names(col)
	gen sex = "female"
	gen coefficient = "ub"
	gen regression = "diff"
	tempfile female_ubcoeffs_diff
	save `female_ubcoeffs_diff'

	// male coefficients from non-logged model
	clear
	matrix colnames b_male_diff = yeardiff 
	svmat b_male_diff, names(col)
	gen sex = "male"
	gen coefficient = "b"
	gen regression = "diff"
	tempfile male_coeffs_diff
	save `male_coeffs_diff'

	clear
	matrix colnames lb_male_diff = yeardiff 
	svmat lb_male_diff, names(col)
	gen sex = "male"
	gen coefficient = "lb"
	gen regression = "diff"
	tempfile male_lbcoeffs_diff
	save `male_lbcoeffs_diff'

	clear
	matrix colnames ub_male_diff = yeardiff 
	svmat ub_male_diff, names(col)
	gen sex = "male"
	gen coefficient = "ub"
	gen regression = "diff"
	tempfile male_ubcoeffs_diff
	save `male_ubcoeffs_diff'

	append using `female_coeffs_diff' `female_lbcoeffs_diff' `female_ubcoeffs_diff' `male_coeffs_diff' `male_lbcoeffs_diff' 
		
saveold "strCoefficientsDirectory/allcoefficients_`date'.dta", replace

log close

