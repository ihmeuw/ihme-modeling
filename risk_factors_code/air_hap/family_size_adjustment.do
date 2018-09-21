// date: 1/11/2017
// purpose: Crosswalk family size for exposure to solid fuel use. Ratio of means is calculated from DHS/MICS data where 
// household size is known to apply across other surveys. Mixed effect regression employed with random effects at super_region,
// region, and country. 

local hap_dir "filepath"
local hap_path "filepath.csv"
adopath + "filepath"

di in red "`hap_dir'"
	//append new sources from ubcov together for later addition -- add adjusted DHS/MICS values from ubcov
	import delimited "`hap_dir'/`hap_path'", clear
	gen ind = 0
	preserve
		use `hap_expose', clear
		drop if mean > 1 & mean < 1000
		rename (mean standard_error) (mean_2016 se_2016)
		save `hap_expose', replace
	restore
	append using `hap_expose'
	replace mean = mean_2016 if mean == . // for ubcov surveys w/o hh_size adjustment
	replace standard_error = se_2016 if standard_error == .
	duplicates tag year_start location_name, gen(dup_tag)
	drop if dup_tag == 1 & ind != 0
	drop if mean == 1 | mean == 0
	tempfile ubcov_sources
	save `ubcov_sources', replace

	//tempfile adjusted values dataset
	import delimited "`hap_dir'/`hap_path'", clear
	rename (mean standard_error) (mean_adj se_adj)
	gen ind = 0
	tempfile adjusted
	save `adjusted', replace

	//merge adjusted and unadjusted and scatter
	use `hap_expose', clear
	drop if mean == 1 | mean == 0
	gen ind = 1
	rename (mean_2016 se_2016) (mean_unadj se_unadj)
	tempfile unadjusted
	save `unadjusted', replace
	merge 1:1 location_name year_start nid using `adjusted', keep(3) nogen
	// graphing to identify outliers 
	// twoway (scatter mean_adj mean_unadj) (function y=x), ytitle(adjusted) xtitle(unadjusted) title (Scatter plot: Adjusted mean and Unadjusted mean by data extraction) // no outliers
	drop if mean_adj<mean_unadj // drop the data points identified 

	// make a list of filepath
	keep nid location_name year_start 
	tempfile matchfile 
	save `matchfile', replace 

	// reduce the adjusted and Undajusted datasets to the matched files 
	use `adjusted',clear 
	merge 1:1 nid location_name year_start using `matchfile', keep(3) nogen
	save `adjusted',replace 

	use `unadjusted',clear 
	merge 1:1 nid location_name year_start using `matchfile', keep(3) nogen
	save `unadjusted', replace 
	append using `adjusted'
	replace mean_unadj = mean_adj if mean_unadj == .

*************************
*****locations***** 
*************************
// Prep location hierarchy file
preserve
	get_location_metadata, location_set_id(22) clear 
	keep location_id region_id super_region_id ihme_loc_id
	tempfile loc 
	save `loc', replace
restore
	
merge m:1 location_id using `loc', keep(3) nogen
keep ihme_loc_id nid field_citation_value year_start year_end mean_unadj se_unadj ind location_id region_id super_region_id

*****************************
*****Crosswalk***** 100 surveys in the crosswalk, excluded those have opposite directions
*****************************
// Run mixed effect model to generate a multiplier for shifting 
	gen mean_orig=mean_unadj
	gen logit_mean = logit(mean_unadj)

	mixed logit_mean ind || super_region_id: ind || region_id: ind || location_id:ind
	predict re*, reffect
	gen coef = re1 + re3 + (_b[ind]) * ind
	// gen mean_adj=invlogit(logit_mean - coef)

	sort nid ind 
	// whether the value in reasonable range  
	// codebook mean_orig mean_adj
	// scatter plot to compare adjusted and unadjusted mean
	// twoway (scatter mean_adj mean_orig if ind==1, sort) (function y=x), ytitle(adjusted) xtitle(unadjusted) title(Scatter Plot: Comparison of pre- and post- crosswalk)

	// percentage change from unadjusted to adjusted 
	// gen change= (mean_adj-mean_orig)/mean_orig*100 
		//the crosswalk reduced the mean 
		drop coef

	gen coef=_b[ind]
	rename (re1 re3) (re_sregion re_region)
	keep region_id super_region_id re_sregion re_region coef logit_mean

	preserve 
	duplicates drop region_id, force 
	tempfile reffect_region
	save `reffect_region', replace // region random effect
	restore 
	duplicates drop super_region_id, force 
	tempfile reffect_sregion
	save `reffect_sregion', replace // super region random effect 

*********************************************************************************
// Cross walk for family size 
*********************************************************************************
use "filepath", clear
replace nid = underlying_nid if nid == .
rename (mean se) (mean_2015 se_2015)
	duplicates drop nid year_start ihme_loc_id, force // check on this later
	tempfile other_sources
	save `other_sources', replace

use `ubcov_sources', clear
append using `other_sources', gen(label)
duplicates tag nid location_name year_start, gen(tag)
drop if tag > 0 & label == 1
duplicates drop location_name year_start nid, force // kenya has a dup 
drop if mean == 1 | mean == 0
rename mean mean_ubcov
rename standard_error se_ubcov

// get region and super_region id 
merge m:1 location_id using `loc', keepusing(region_id super_region_id) keep(3) nogen
// get random effect at region level 
merge m:1 region_id using `reffect_region', keepusing (re_region) keep(1 3) nogen
// replace region random effect to zero for those regions do not have re_region
replace re_region=0 if re_region==.
// get random effect at super region level 
merge m:1 super_region_id using `reffect_sregion', keepusing (re_sregion coef) keep(1 3) nogen
// replace region random effect to zero for those regions do not have re_region
replace re_sregion=0 if re_sregion==.

summarize coef
replace coef = `r(mean)' if coef == .
replace ind=1 if ind !=0
// generate multiplier 
gen multiplier = re_sregion + re_region + coef * ind 
replace mean_2016 = mean_ubcov
replace mean_2016 = mean_2015 if mean_2016 == .
replace se_2016 = se_ubcov
replace se_2016 = se_2015 if se_2016 == .
drop if mean_2016 == 1 | mean_2016 == 0
gen logit_mean = logit(mean_2016)

**************************************************
// Crosswalk
***************************************************
gen mean_adj=invlogit(logit_mean - multiplier)
replace mean_2016=mean_adj if ind==1 
drop mean_adj re_region re_sregion coef multiplier

***************************************************
// MERGE ON COVARIATES
***************************************************
local covariates prop_urban
// ldi_pc education_yrs_pc sds
foreach covar of local covariates {
	preserve
	get_covariate_estimates, covariate_name_short("`covar'") clear
	capture duplicates drop location_id year_id, force
	tempfile `covar'
	save ``covar'', replace
	restore
	merge m:1 location_id year_id using "``covar''", nogen keepusing(mean_value) keep(1 2 3)
	rename mean_value `covar'
	}
*****************************************
// clean up
*****************************************
replace field_citation_value = filepath if field_citation_value == ""
keep mean_2016 se_2016 ihme_loc_id year_start year_end field_citation_value location_id location_name nid
gen variance = se_2016^2
gen age_group_id = 22
gen sex_id = 3

//prep for ST-GPR
gen me_name = "air_hap"
drop if data <= 0 | data >= 1 
gen sample_size = .
rename (mean_2016 year_start) (data year_id)

// Outliering 
replace data = . if nid == 69854 & year_id == 1997 & ihme_loc_id == "IND"
replace data = . if nid == 19410 & year_id == 1991 & ihme_loc_id == "DOM"
replace data = . if nid == 39444 & year_id == 1991 & ihme_loc_id == "JAM"
replace data = . if nid == 69854 & year_id == 2001 & ihme_loc_id == "PSE"
replace data = . if nid == 69854 & year_id == 1999 & ihme_loc_id == "AGO"
replace data = . if nid == 672 & year_id == 2007 & ihme_loc_id == "AGO"
replace data = . if nid == 69854 & year_id == 1996 & ihme_loc_id == "DJI"
replace data = . if nid == 20322 & year_id == 2000 & ihme_loc_id == "MRT"

*********************************************
// Outliers ..2015
*********************************************
replace data = . if nid==5407 & ihme_loc_id=="IDN"
replace data = . if nid==141558 & ihme_loc_id=="THA"
replace data = . if nid==11410 & ihme_loc_id=="WSM"
replace data = . if nid==797 & ihme_loc_id=="ARM"
replace data = . if nid==811 & ihme_loc_id=="ARM"
replace data = . if nid==21676 & ihme_loc_id=="KAZ"
replace data = . if nid==189045 & ihme_loc_id=="MNG"
replace data = . if nid==11214 & ihme_loc_id=="RUS"
replace data = . if nid==11271 & ihme_loc_id=="RUS"
replace data = . if nid==11299 & ihme_loc_id=="RUS"
replace data = . if nid==7480 & ihme_loc_id=="KOR"
replace data = . if nid==160478 & ihme_loc_id=="KOR"
replace data = . if nid==7481 & ihme_loc_id=="KOR"
replace data = . if nid==10371 & ihme_loc_id=="PRY"
replace data = . if nid==141628 & ihme_loc_id=="PSE"
replace data = . if nid==45777 & ihme_loc_id=="IND"
replace data = . if nid==60942 & ihme_loc_id=="PAK"
replace data = . if nid==154210 & ihme_loc_id=="AGO"
replace data = . if nid==3736 & ihme_loc_id=="ETH"
replace data = . if nid==208731 & ihme_loc_id=="NGA"
replace data = . if nid==9492 & ihme_loc_id=="NGA"

//3/17/2016
replace data = . if nid==34012 & ihme_loc_id=="ARG" // IPUMS, the options for fuel only include GAS, which makes the prevalence 0
replace data = . if nid==81044 & ihme_loc_id=="IND" // In the report, only urban, rural split for country level, replace data = . 
replace data = . if nid==124072 // China subnational
replace data = . if nid==798 & ihme_loc_id=="ARM" // not much information on GHDx
replace data = . if nid==794 & ihme_loc_id=="ARM" // not much information on GHDx
replace data = . if nid==802 & ihme_loc_id=="ARM" // do not have fuel variable 
replace data = . if nid==22790 & ihme_loc_id=="ARM" // do not have fuel variable 
replace data = . if nid==22813 & ihme_loc_id=="ARM" // do not have fuel variable 
replace data = . if nid==22817 & ihme_loc_id=="ARM" // do not have fuel variable 
replace data = . if nid==22786 & ihme_loc_id=="ARM" // do not have fuel variable
replace data = . if nid==142027 & ihme_loc_id=="ARM" // Data actually from Households Integrated Living Conditions Survey 
replace data = . if nid==22786 & ihme_loc_id=="ARM" // do not have fuel variable
replace data = . if nid==154210 & ihme_loc_id=="SDN" // The report is more national level, instead of surveys, replace data = . for now and look into it later 
replace data = . if nid==23219 // India DHLS, the option of "other" in the fuel use may include solid fule other than wood. So the level is underestimate. replace data = . for both national and subnational 
// 4/24/2016
replace data = . if nid==141571 & ihme_loc_id=="SEN" // the percentage was only for firewood, potentially underestimated comparing with the adjascent years
replace data = . if nid==56420 & ihme_loc_id=="KEN" // Implausibly low
