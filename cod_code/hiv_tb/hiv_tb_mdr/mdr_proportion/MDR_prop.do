** Description: calculating proportions of MDR-TB among HIV-positive and HIV-negative people

// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "ADDRESS"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "ADDRESS"
				}
			
			// Close any open log file
				cap log close
				
	

// pull TB all forms
clear all
adopath + "ADDRESS"
get_model_results, gbd_team("epi") gbd_id(9806) location_set_id(22) clear
keep if measure_id==6
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
duplicates drop location_id year_id age_group_id sex_id measure_id, force
drop lower upper
rename mean tb_inc
tempfile tb
save `tb', replace


// get population
	  	
		clear all
		adopath + "ADDRESS"
		get_population, location_id("-1") year_id("-1") sex_id("1 2") age_group_id("-1") location_set_id(22) clear
		tempfile pop_all
		save `pop_all', replace
		

// get HIV-TB incidence

get_model_results, gbd_team("epi") gbd_id(1176) location_set_id(22) clear
keep if measure_id==6
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
duplicates drop location_id year_id age_group_id sex_id measure_id, force
drop lower upper
rename mean hiv_tb_inc

tempfile hiv_tb
save `hiv_tb', replace

// get TB no-HIV incidence

get_model_results, gbd_team("epi") gbd_id(9969) location_set_id(22) clear
keep if measure_id==6
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
duplicates drop location_id year_id age_group_id sex_id measure_id, force
drop lower upper
rename mean tbnoHIV_inc

tempfile tb_noHIV
save `tb_noHIV', replace

// merge the files 

use `tb', clear
merge 1:1 location_id year_id age_group_id sex_id using `hiv_tb', keep(3)nogen
merge 1:1 location_id year_id age_group_id sex_id using `tb_noHIV', keep(3)nogen

// Prepare to project back to 1980
expand 2 if year_id==1990, gen(exp)
replace year=1980 if exp==1
// drop exp 
drop exp 


// Interpolate / extrapolate for years for which we don't have results
egen panel = group(location_id age_group_id sex_id)
tsset panel year_id
tsfill, full
bysort panel: egen pansex = max(sex_id)
bysort panel: egen panage = max(age_group_id)
replace sex_id = pansex
drop pansex
replace age_group_id=panage
drop panage

foreach metric in "tb_inc" "hiv_tb_inc" "tbnoHIV_inc" {
	bysort panel: ipolate `metric' year, gen(`metric'_new) epolate
}

bysort panel: replace location_id = location_id[_n-1] if location_id==.

bysort panel: replace age_group_id = age_group_id[_n-1] if age_group_id==.

bysort panel: replace sex_id = sex_id[_n-1] if sex_id==.

merge 1:1 location_id year_id age_group_id sex_id using `pop_all', keepusing(population) keep(3)nogen

save "FILEPATH", replace


************************************************************************************************************

// calculate ratio of tb_noHIV to hiv_tb

use "FILEPATH", clear

// calculate ratios

replace tb_inc = tb_inc_new
replace hiv_tb_inc = hiv_tb_inc_new
replace tbnoHIV_inc = tbnoHIV_inc_new

drop *_new

gen ratio=hiv_tb_inc/tbnoHIV_inc 
			
// calculate all MDR cases

merge m:1 location_id year_id using "FILEPATH", keep(3)nogen

/* gen rr=1.22882 */

// calculate all MDR cases at the 1000 draw level

	forvalues x = 0/999 {
		gen mdr_all_`x' = tb_inc*population*mean_prop_`x'
				}
	drop mean_prop_*


// calculate hiv_mdr cases as hiv_mdr=(rr/(tbnoHIV/hiv_tb)*(all_tb*MDR_prop)/(rr/(tbnoHIV/hiv_tb)+1)

gen acause="tb_drug"

merge m:1 acause using "FILEPATH", keep(3)nogen


forvalues x = 0/999 {
		gen tb_nohiv_mdr_cases_`x' = mdr_all_`x'/(1+(rr_`x'*ratio))
				}
drop mdr_all_*					


// calculate TB no-HIV cases

gen tb_noHIV_cases= tbnoHIV_inc*population

forvalues x = 0/999 {
		gen tb_nohiv_mdr_prop_`x' = tb_nohiv_mdr_cases_`x'/tb_noHIV_cases
				}

keep location_id year_id age_group_id sex_id tb_nohiv_mdr_prop_* rr_*

tempfile tb_nohiv_mdr
save `tb_nohiv_mdr', replace

drop rr_*

save "FILEPATH", replace

// calculate HIVTB mdr prop

use `tb_nohiv_mdr', clear

forvalues x = 0/999 {
		gen hiv_mdr_prop_`x' = tb_nohiv_mdr_prop_`x'*rr_`x'
				}

keep location_id year_id age_group_id sex_id hiv_mdr_prop_*

save "FILEPATH", replace
		
