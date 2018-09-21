** ********************************************************************************************************************************************************************
** 	Adult Mortality through Sibling Histories: #5. CONVERTING MALE/FEMALE RESULTS TO A CLEARER FORMAT
**
**   Description: This do-file converts the male and female results into a graphable format.		
**  	Input:
**			1) finalmodel_allcountrypool_1.dta, finalmodel_allcountrypool_0.dta
**			2) zero-female-survivor_correction_factors.dta	
**		Steps: 
**				1. Append male and female results together
**				2. Create year and period variables
**				3. Rename and label variables and save final output dataset
**
**		Output: 45q15 corresponding uncertainty bounds for each CY included in analysis, for both males and females, identified by the country and the year interval 
**		File: fullmodel_finalresults.dta
**		Variables in the output dataset: country male midyear interval_bgn interval_end interval_width sib45q15 lb_sib45q15 ub_sib45q15 
**
**  NOTE: IHME OWNS THE COPYRIGHT
**
** ********************************************************************************************************************************************************************

** ***************************************************************************************
** SET UP STATA							
** ***************************************************************************************
clear
capture clear matrix
set mem 50m
set more off
pause on

local date = c(current_date)

insheet using "$datadir/sibhistlist.csv", c clear names

levelsof svy, local(svys)
tempfile all

foreach s of local svys {

** ***************************************************************************************
** 1. Append male and female results together 					
** ***************************************************************************************

** ********************************************************************************************************************
// DECISION POINT: if respondents are interviewed about only sisters or only brothers, you will only have one sex here 
** ********************************************************************************************************************

use "$datadir/finalmodel_allcountrypool_1_`s'.dta", clear
rename male_45q15 sib45q15
rename male_lgt45q15 lgt_sib45q15
rename sd_lgt45q15_1 sd_lgt45q15
rename lb_lgt45q15_1 lb_lgt45q15
rename ub_lgt45q15_1 ub_lgt45q15
renpfix male_ ""
tempfile male
save `male', replace

use "$datadir/finalmodel_allcountrypool_0_`s'.dta", clear
rename female_45q15 sib45q15
rename female_lgt45q15 lgt_sib45q15
rename sd_lgt45q15_0 sd_lgt45q15
rename lb_lgt45q15_0 lb_lgt45q15
rename ub_lgt45q15_0 ub_lgt45q15
renpfix female_ ""

append using `male'
gen svy = substr("`s'",6,.)
capture append using `all'
save `all', replace
}

** ***************************************************************************************
** 2. Create year and period variables, save dataset					
** ***************************************************************************************

gen yr = substr(svy_yr,-4,.)								// Beginning of the time period
gsort sex svy -yr

by sex svy: gen period = _n

gen female = 1 - sex
order svy female period sib45q15 lb_45q15 ub_45q15
keep svy female period sib45q15 lb_45q15 ub_45q15


save "$datadir/fullmodel_svy.dta", replace

