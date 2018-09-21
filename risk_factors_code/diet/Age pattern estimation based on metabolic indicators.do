// Compile FAO age trend results and apply them
clear all
macro drop _all
set maxvar 32000
// Set to run all selected code without pausing
set more off
// Remove previous restores
cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
if c(os) == "Unix" {
	global j "FILEPATH"
	set odbcmgr unixodbc
}
else if c(os) == "Windows" {
	global j "FILEPATH"
}

local population 	"FILEPATH"
local raw_data		"FILEPATH"


**************************************************************************************
********************* Prepare those that will be averaged ****************************
**************************************************************************************
**Averaging for fruit - ihd
use `sbp_ihd_60', clear
	merge 1:1 age_group_id using `bmi_ihd_60', nogen
	merge 1:1 age_group_id using `fpg_ihd_60', nogen
	merge 1:1 age_group_id using `cholesterol_ihd_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_fruit_cvd_ihd
		save `diet_fruit_cvd_ihd', replace

**Averaging for fruit - isch_stroke
use `bmi_isch_stroke_60', clear
	merge 1:1 age_group_id using `sbp_isch_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_isch_stroke_60', nogen
	merge 1:1 age_group_id using `cholesterol_isch_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_fruit_cvd_stroke_isch
		save `diet_fruit_cvd_stroke_isch', replace

**Averaging for fruit - hem_stroke
use `sbp_hem_stroke_60', clear
	merge 1:1 age_group_id using `bmi_hem_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_hem_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_fruit_cvd_stroke_cerhem
		save `diet_fruit_cvd_stroke_cerhem', replace

**Averaging for fruit - diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		tempfile diet_fruit_diabetes
		save `diet_fruit_diabetes', replace

**Averaging for veg - ihd
use `sbp_ihd_55', clear
	merge 1:1 age_group_id using `bmi_ihd_55', nogen
	merge 1:1 age_group_id using `fpg_ihd_55', nogen
	merge 1:1 age_group_id using `cholesterol_ihd_55', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_veg_cvd_ihd
		save `diet_veg_cvd_ihd', replace

**Averaging for veg - isch_stroke
use `sbp_isch_stroke_60', clear
	merge 1:1 age_group_id using `bmi_isch_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_isch_stroke_60', nogen
	merge 1:1 age_group_id using `cholesterol_isch_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_veg_cvd_stroke_isch
		save `diet_veg_cvd_stroke_isch', replace

**Averaging for veg - hem_stroke
use `sbp_hem_stroke_60', clear
	merge 1:1 age_group_id using `bmi_hem_stroke_60', nogen
	merge 1:1 age_group_id using `fpg_hem_stroke_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_veg_cvd_stroke_cerhem
		save `diet_veg_cvd_stroke_cerhem', replace

**Averaging for nuts - ihd
use `cholesterol_ihd_55', clear
	**merge 1:1 age_group_id using `sbp_ihd_55', nogen
	merge 1:1 age_group_id using `fpg_ihd_55', nogen
	merge 1:1 age_group_id using `bmi_ihd_55', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_nuts_cvd_ihd
		save `diet_nuts_cvd_ihd', replace

**Averaging for nuts - diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		tempfile diet_nuts_diabetes
		save `diet_nuts_diabetes', replace

**Averaging for grains - ihd
use `bmi_ihd_65', clear
	**merge 1:1 age_group_id using `sbp_ihd_65', nogen
	merge 1:1 age_group_id using `fpg_ihd_65', nogen
	merge 1:1 age_group_id using `cholesterol_ihd_65', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_grains_cvd_ihd
		save `diet_grains_cvd_ihd', replace

**Averaging for grains - isch_stroke
use `bmi_isch_stroke_65', clear
	**merge 1:1 age_group_id using `sbp_isch_stroke_65', nogen
	merge 1:1 age_group_id using `fpg_isch_stroke_65', nogen
	merge 1:1 age_group_id using `cholesterol_isch_stroke_65', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_grains_cvd_stroke_isch
		save `diet_grains_cvd_stroke_isch', replace

**Averaging for grains - hem_stroke
use `bmi_hem_stroke_65', clear
	**merge 1:1 age_group_id using `sbp_hem_stroke_65', nogen
	merge 1:1 age_group_id using `fpg_hem_stroke_65', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_grains_cvd_stroke_cerhem
		save `diet_grains_cvd_stroke_cerhem', replace

**Averaging for grains - diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		tempfile diet_grains_diabetes
		save `diet_grains_diabetes', replace

**Averaging for redmeat - diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		tempfile diet_redmeat_diabetes
		save `diet_redmeat_diabetes', replace

**Averaging for procmeat - ihd
use `bmi_ihd_60', clear
	merge 1:1 age_group_id using `sbp_ihd_60', nogen
	merge 1:1 age_group_id using `fpg_ihd_60', nogen
	**merge 1:1 age_group_id using `cholesterol_ihd_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_procmeat_cvd_ihd
		save `diet_procmeat_cvd_ihd', replace

**Averaging for procmeat - diabetes
use `bmi_dm_60', clear
	gen percentage_change = percentage_change_bmi
		keep risk age_group_id percentage_change
		tempfile diet_procmeat_diabetes
		save `diet_procmeat_diabetes', replace

**Averaging for pufa - ihd
use `cholesterol_ihd_50', clear
	**merge 1:1 age_group_id using `sbp_ihd_50', nogen
	merge 1:1 age_group_id using `fpg_ihd_50', nogen
	**merge 1:1 age_group_id using `bmi_ihd_50', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_pufa_cvd_ihd
		save `diet_pufa_cvd_ihd', replace

**Averaging for fish - ihd
use `bmi_ihd_60', clear
	merge 1:1 age_group_id using `sbp_ihd_60', nogen
	**merge 1:1 age_group_id using `fpg_ihd_60', nogen
	**merge 1:1 age_group_id using `cholesterol_ihd_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_fish_cvd_ihd
		save `diet_fish_cvd_ihd', replace

**Averaging for transfat - ihd
use `cholesterol_ihd_60', clear
	**merge 1:1 age_group_id using `sbp_ihd_60', nogen
	**merge 1:1 age_group_id using `fpg_ihd_60', nogen
	merge 1:1 age_group_id using `bmi_ihd_60', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_transfat_cvd_ihd
		save `diet_transfat_cvd_ihd', replace

**Averaging for fiber - ihd
use `cholesterol_ihd_50', clear
	**merge 1:1 age_group_id using `sbp_ihd_50', nogen
	**merge 1:1 age_group_id using `fpg_ihd_50', nogen
	**merge 1:1 age_group_id using `bmi_ihd_50', nogen
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_fiber_cvd_ihd
		save `diet_fiber_cvd_ihd', replace

**Averaging for legumes - ihd
use `cholesterol_ihd_60', clear
	egen percentage_change = rowmean(percentage_change*)
		keep risk age_group_id percentage_change
		tempfile diet_legumes_cvd_ihd
		save `diet_legumes_cvd_ihd', replace


**************************************************************************************
**************************************************************************************
**************************************************************************************


capture restore, not
import excel "`raw_data'", firstrow clear
**only keep those we are interested in
drop if removing == 1 | morbidity == . | mortality == . | pending == 1 | outcome == "metab_bmi"

**do this one separate since separate risks for morbidity versus mortality
drop if risk == "diet_nuts" & outcome == "cvd_ihd"

**drop salt and ssb
drop if risk == "diet_salt_direct" | risk == "diet_ssb_direct"

drop if Reference == "WCRF"

**prepare locals of each pair
levelsof risk, local(risks)
levelsof outcome, local(outcomes)

foreach risk of local risks {
foreach outcome of local outcomes {
preserve
**for testing purposes
keep if risk == "`risk'" & outcome == "`outcome'"

count
if `r(N)' != 0 {	

**drop unnecessary vars
drop new* removing pending parameter unit Reference

gen standard_error = (rr_upper - rr_lower)/(2*1.96)
gen location_id = 1

**rename grams_daily exp_mean
rename rr_mean exp_mean
forvalues n = 0/999 {
	gen rr_`n' = rnormal(exp_mean, standard_error)
	replace rr_`n' = log(rr_`n')
}

**associate the metabolic age trends where appropriate
	local risk = risk
	replace risk = "."
	cap joinby risk using ``risk'_`outcome''
	replace risk = "`risk'"

quietly {
forvalues n = 0/999 {
	capture replace rr_`n' = rr_`n' * percentage_change
	replace rr_`n' = exp(rr_`n')
}
}
cap drop percentage_change exp_mean rr_lower rr_upper standard_error location_id year_id

egen mean_rr = rowmean(rr_*)
cap mkdir "FILEPATH"
save "FILEPATH", replace
}
restore
}
}

**take care of nuts - cvd_ihd separately 
import excel "`raw_data'", firstrow clear

**do this one separate since separate risks for morbidity versus mortality
keep if risk == "diet_nuts" & outcome == "cvd_ihd"

levelsof morbidity, local(levels)

foreach level of local levels{
	preserve
	keep if morbidity == `level'

**drop unnecessary vars
drop new* removing pending parameter unit Reference

gen standard_error = (rr_upper - rr_lower)/(2*1.96)
gen location_id = 1
gen year_id = 2015
**rename grams_daily exp_mean
rename rr_mean exp_mean
forvalues n = 0/999 {
	gen rr_`n' = rnormal(exp_mean, standard_error)
	replace rr_`n' = log(rr_`n')
}
**Redmeat and Diabetes
	local risk = risk
	replace risk = "."
	joinby risk using `diet_nuts_cvd_ihd'
	replace risk = "`risk'"

quietly {
forvalues n = 0/999 {
	replace rr_`n' = rr_`n' * percentage_change
	replace rr_`n' = exp(rr_`n')
}
}
drop percentage_change exp_mean rr_lower rr_upper standard_error location_id year_id

tempfile level_`level'
save `level_`level'', replace
restore
}
use `level_1', clear
	append using `level_0' 
	egen mean_rr = rowmean(rr_*)
cap mkdir "FILEPATH"
save "FILEPATH", replace



*********************************
**expand cancers separately
*********************************
capture restore, not
import excel "`raw_data'", firstrow clear
keep if Reference == "WCRF"

drop if risk == "diet_salt_direct"

**prepare locals of each pair
levelsof risk, local(risks)
levelsof outcome, local(outcomes)
**local risks "diet_fiber"
**local outcomes "neo_prostate"

foreach risk of local risks {
foreach outcome of local outcomes {
preserve
**for testing purposes
keep if risk == "`risk'" & outcome == "`outcome'"

count
if `r(N)' != 0 {	

**drop unnecessary vars
drop new* removing pending parameter unit Reference

gen standard_error = (rr_upper - rr_lower)/(2*1.96)
gen location_id = 1

forvalues n = 0/999 {
	gen rr_`n' = rnormal(rr_mean, standard_error)
}

expand 12
**create age_group_id var
gen obs = _n
replace obs = obs + 9
rename obs age_group_id

cap drop percentage_change rr_mean rr_lower rr_upper standard_error location_id year_id

egen mean_rr = rowmean(rr_*)
cap mkdir "FILEPATH"
save "FILEPATH", replace
}
restore
}
}