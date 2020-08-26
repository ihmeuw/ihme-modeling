// NAME
// February 2014
// Subtract background mortality from prepped KM data to get HIV-specific mortality

**************************************************************
** SET UP
**************************************************************
clear all
set more off
cap log close
cap restore, not


// Initialize pdfmaker
if c(os) == "Windows" {
	global prefix "ADDRESS"
	do "FILEPATH"
}
if c(os) == "Unix" {
	global prefix "ADDRESS"
	do "FILEPATH"
	set odbcmgr unixodbc
}

// Filepaths
local GBD_pop "FILEPATH"
// local HIV_free_mort "FILEPATH"
local HIV_free_mort "FILEPATH"

local extract_dir "FILEPATH"
local figure_dir "FILEPATH"
local regress_dir "FILEPATH"

// Toggles
local visuals = 1

// Temporary mapping from gbd2013 iso3 to gbd2015 for merging
adopath + "FILEPATH"
get_locations
keep if local_id_2013 != ""
keep local_id_2013 ihme_loc_id
rename local_id_2013 iso3
tempfile locations
save `locations' 

//need a list of location_ids 
get_locations
drop if regexm(ihme_loc_id,"_") & !inlist(ihme_loc_id,"CHN_44533", "CHN_354","CHN_361")
levelsof location_id , local(locids)
tempfile loc_map 
save `loc_map', replace

//age mape 
get_age_map, type("all") 
tempfile age_map 
save `age_map', replace

**************************************************************
**PREP KAPLAN-MEIER MORTALITY DATA
**************************************************************
// Bring in ALPHA, ZAF, and Weibull data
insheet using "`extract_dir'/compiled/weibull_alpha_zaf.csv", clear comma names
drop if yr_since_sc > 12
append using "`extract_dir'/prepped/cum_mort_all_cause.dta"
drop logit_mort


// Sex
generate s = "male" if sex == 1
replace s = "female" if sex == 2
replace s = "both" if sex == 3
drop sex
rename s sex

// Age groups
tostring age_start, generate(x)
tostring age_end, generate(y)
generate age_group = x+"_"+y
drop x y

// Convert to conditional probabilities of death - NOTE THIS IS FRAME SHIFTED FORWARD!!!
sort pubmed_id iso3 year_start year_end age_start age_end sex yr_since_sc
by pubmed_id iso3 year_start year_end age_start age_end sex: generate cond_prob_death = ((mort-mort[_n-1])/(1 - mort[_n-1])) if yr_since_sc != 1
replace cond_prob_death = mort if yr_since_sc == 1

// Conditional probability has to be between 0 and 1
replace cond_prob_death = 0 if cond_prob_death < 0

// Convert to conditional death rates using life table equation and nax = 0.5
generate cond_death_rate = cond_prob_death/(1-0.5*cond_prob_death)

// All age groups - use crude death rates
generate all_ages = (inlist(age_start, 0, 15, 18) & age_end == 100)

// Exact age interval - this means that the age interval in teh data matches or is very close to one of the age-intervals in Haidong's life tables (ie, 5-year).  There's no point to averaging surrounding age interval mortality rates
generate exact_age_interval = 1 if inlist(age_group, "25_29", "21_23")

// Age-specific - use midpoint age, then the surrounding two five-year age groups from Haidong's life tables
drop midpoint_age
replace age_end = 65 if age_end == 100 & all_ages != 1
replace age_end = 75 if age_start == 65 & age_end == 65
generate midpoint_age = (age_end + age_start)/2
replace midpoint_age = . if all_ages == 1

// Age interval
generate age_range = age_end-age_start if all_ages != 1

// Age groups
generate age_group_floor = 5*floor(age_start/5) if all_ages != 1
generate age_group_ceiling = 5*floor(age_end/5) if all_ages != 1
replace age_group_floor = age_group_floor + 5 if yr_since_sc >= 5 & yr_since_sc < 10 & all_ages != 1
replace age_group_ceiling = age_group_ceiling + 5 if yr_since_sc >= 5 & yr_since_sc < 10 & all_ages != 1
replace age_group_floor = age_group_floor + 10 if yr_since_sc >= 10 & yr_since_sc < 15 & all_ages != 1
replace age_group_ceiling = age_group_ceiling + 10 if yr_since_sc >= 10 & yr_since_sc < 15 & all_ages != 1

// Midpoint year
generate year = floor((year_start+year_end)/2)
replace year = year + 5 if yr_since_sc >= 5 & yr_since_sc < 10 
replace year = year + 10 if yr_since_sc >= 10

// ID for merge
generate id = iso3
replace id = "HIGH" if iso3 == "" 

//special for new UNAIDS data
replace id = "HIGH" if id == "UNAIDs E/NA"


tempfile prepped
save `prepped', replace


**************************************************************
**PREP HIV-FREE MORTALITY DATA FROM HAIDONG
**************************************************************
// Merge on crude death rates for all ages data
insheet using "`HIV_free_mort'", clear
merge m:1 age_group_id using `age_map', keep(1 3) keepusing(age_group_years_start) nogen 
rename age_group_years_start age 
drop age_group_id
drop if regexm(ihme_loc_id,"_") & !inlist(ihme_loc_id,"CHN_44533", "CHN_354","CHN_361") // Don't double-count subnational locations
rename ihme_loc_id iso3
rename mean_mx mx_nohiv_mean

gen sex_new = ""
replace sex_new = "male" if sex == 1
replace sex_new = "female" if sex == 2
drop sex 
rename sex_new sex

reshape wide mx_nohiv_mean, i(iso3 sex year) j(age)
gen id = iso3

tempfile mort_rates
save `mort_rates', replace

**************************************************************
**PREP GBD POPULATION DATA
**************************************************************
*use `GBD_pop' if year_id >= 1970 & sex != "both", clear   
get_population, location_id("`locids'") sex_id("1 2") status("recent")  /// 
year_id("1970 1971 1972 1973 1974 1975 1976 1977 1978 1979 1980 1981 1982 1983 1984 1985 1986 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017") ///
age_group_id("-1") clear

merge m:1 age_group_id using `age_map', keep(1 3) nogen 
merge m:1 location_id using `loc_map', keep(1 3) nogen

rename year_id year 
drop if missing(ihme)
// Prep GBD populations to be in same format as GBD2013 for seamless merging
replace age_group_name = subinstr(age_group_name," to ","_",.)
replace age_group_name = subinstr(age_group_name," plus","_",.)
replace age_group_name = "0" if age_group_id == 28

drop if inlist(age_group_name,"All Ages","Neonatal", "Under 5") | inlist(age_group_id,2,3,4) | regexm(age_group_name, "years") 

keep year sex ihme_loc_id age_group_name pop
rename (ihme_loc_id pop) (iso3 pop_)

reshape wide pop_, i(iso3 year sex) j(age_group_name) string
rename sex_id sex

tostring sex, replace 
replace sex = "male" if sex == "1" 
replace sex = "female" if sex == "2"

merge 1:1 iso3 year sex using `mort_rates'
keep if _m == 3

// Rough estimate of high income no HIV mortality.  Need to do this with population too  so we can get crude death rates
preserve
keep if inlist(iso3, "USA", "GBR", "BEL", "AUT", "FRA") | inlist(iso3, "CAN", "ITA", "GRC", "NLD", "ESP")
collapse (mean) mx_nohiv_* (sum) pop*, by(sex year)
generate id = "HIGH"
tempfile high_income
save `high_income', replace
restore

// Rough estimate of ALPHA network mortality
preserve
keep if inlist(iso3, "UGA", "TZA")
collapse (mean) mx_nohiv_* (sum) pop*, by(sex year)
generate id = "ALPHA"
tempfile alpha
save `alpha', replace
restore

// Rough estimate of Weibull mortality
preserve
keep if inlist(iso3, "UGA", "TZA", "ZAF")
collapse (mean) mx_nohiv_* (sum) pop*, by(sex year)
generate id = "WEIBULL"
tempfile weibull
save `weibull', replace
restore

// Rough estimate of Unaids Africa mortality
preserve
keep if inlist(iso3, "UGA", "TZA", "ZAF","MWI") | inlist(iso3,"KEN","BWA","ZMB","CIV","ZWE","RWA")
collapse (mean) mx_nohiv_* (sum) pop*, by(sex year)
generate id = "UNAIDs Africa"
tempfile una
save `una', replace
restore

// Rough estimate of Unaids Asia mortality
preserve
keep if inlist(iso3, "CHN_44533","SGP","THA")
collapse (mean) mx_nohiv_* (sum) pop*, by(sex year)
generate id = "UNAIDs Asia"
tempfile unas
save `unas', replace
restore

append using `high_income'
append using `alpha'
append using `weibull'
append using `una'
append using `unas'
tempfile pop_mort
save `pop_mort', replace

**************************************************************
**AGGREGATE ACROSS SEXES TO CREATE BOTH SEXES MORTALITY RATES
**************************************************************
drop _m pop_0 pop_1_4 pop_5_9 mx_nohiv_mean0-mx_nohiv_mean5 mx_nohiv_mean85-mx_nohiv_mean110 pop_85_89 pop_90_94 pop_95_
order id iso3

local age_lower = 10
local age_upper = 80

forvalues i = `age_lower'(5)`age_upper' {
	local j = `i'+4
	rename pop_`i'_`j' pop`i'
}

reshape wide pop`age_lower'-mx_nohiv_mean`age_upper', i(id iso3 year) j(sex) string

forvalues i = `age_lower'(5)`age_upper' {
	generate pop`i'both = pop`i'male + pop`i'female
	generate mx_nohiv_mean`i'both = (pop`i'male*mx_nohiv_mean`i'male + pop`i'female*mx_nohiv_mean`i'female)/pop`i'both
}

**************************************************************
**AGGREGATE ACROSS AGES TO CREATE CRUDE DEATH RATES FOR ALL THREE SEXES
**************************************************************
local sexes "male female both"
foreach sex of local sexes {
	generate popall`sex' = 0
	generate mort_wt_num_`sex' = 0
}

// Some population numbers are missing, especially in the older age groups in lower income countries.  We'll set missings to zeroes here, as all we're doing is computing weighted averages so it won't affect the final results.  We'll also set the corresponding mortality rates to zero.

foreach sex of local sexes {
	forvalues i = `age_lower'(5)`age_upper' {
		replace pop`i'`sex' = 0 if pop`i'`sex' == .
		replace mx_nohiv_mean`i'`sex' = 0 if pop`i'`sex' == 0
		replace popall`sex' = popall`sex'+pop`i'`sex'
		replace mort_wt_num_`sex' = mort_wt_num_`sex' + mx_nohiv_mean`i'`sex'*pop`i'`sex'
	}
}

foreach sex of local sexes {
	generate mx_nohiv_meanall`sex' = mort_wt_num_`sex'/popall`sex'
}

drop mort_wt_num_*
order _all, sequential
order id iso3 year

// Store stubs to reshape long with unab command
unab vars : *both
local stubs : subinstr local vars "both" "", all
reshape long `stubs', i(id iso3 year) j(sex) string

drop iso3 pop*

tempfile all_mort_rates
save `all_mort_rates', replace

**************************************************************
**MERGE ONTO KAPLAN-MEIER MORTALITY DATA
**************************************************************
use `prepped', clear
merge m:1 year sex id using `all_mort_rates' 
count if _m == 1 
if `r(N)' != 0 { 
	do "some cohorts are not assigned bg mort " 
}
			
keep if _m == 3
drop _m

**************************************************************
**ADJUST KAPLAN-MEIER MORTALITY BY SUBTRACTING HAIDONG'S HIV-FREE MORTALITY RATES
**************************************************************
// All ages mortality
generate bg_mort = mx_nohiv_meanall if all_ages == 1

// Exact age interval
levelsof age_group_floor if exact_age_interval == 1, local(floors)
levelsof age_group_ceiling if exact_age_interval == 1, local(ceilings)
foreach f of local floors {
	replace bg_mort = mx_nohiv_mean`f' if exact_age_interval == 1
}

// Wider age interval than Haidong's 5-year rates
levelsof age_group_floor, local(floors)
levelsof age_group_ceiling, local(ceilings)
foreach f of local floors {
	foreach c of local ceilings {
		if `f' < `c' {
			egen x = rowmean(mx_nohiv_mean`f'-mx_nohiv_mean`c')
			replace bg_mort = x if bg_mort == . & age_group_floor == `f' & age_group_ceiling == `c'
			drop x
		}
	}
}
	
// Adjusted mortality
generate death_rate_adj = cond_death_rate - bg_mort
replace death_rate_adj = 0 if death_rate_adj < 0

tempfile adjusted
save `adjusted', replace

**************************************************************
**REGENERATE CUMULATIVE SURVIVAL CURVES
**************************************************************
// Convert from conditional mortality rates to conditional probabilities of death
generate cond_prob_death_adj = death_rate_adj/(1+0.5*death_rate_adj)

// Keep relevant variables
local vars_to_keep "num_study pubmed_id year sex high_income age_cat id yr_since_sc age_group all_ages cond_prob_death_adj death_rate_adj mort iso3"
keep `vars_to_keep'

// Convert back to cumulative mortality
generate cum_mort_adj = cond_prob_death_adj
sort id pubmed_id num_study sex age_group yr_since_sc
bysort id pubmed_id num_study sex age_group: replace cum_mort_adj = cond_prob_death_adj + cum_mort_adj[_n-1] - cond_prob_death*cum_mort_adj[_n-1] if yr_since_sc != 1

tempfile all
save `all', replace

**************************************************************
**OUTSHEET WEIBULL, MINERS, AND ALPHA FOR GRAPHING IN R
**************************************************************
preserve
generate surv = 1 - cum_mort_adj
keep if inlist(pubmed_id, 1111, 2222, 3333)
keep yr_since_sc surv iso3 age_cat
generate age = "15_25" if age_cat == 1
replace age = "25_35" if age_cat == 2
replace age = "35_45" if age_cat == 3
replace age = "45_100" if age_cat == 4
drop age_cat

outsheet using "`extract_dir'/prepped/hiv_specific_weibull_alpha_zaf.csv", replace comma names
restore

**************************************************************
**OUTSHEET UNAIDS 2017
**************************************************************
preserve
generate surv = 1 - cum_mort_adj
keep if inlist(pubmed_id, 28301424)
keep yr_since_sc surv iso3 age_cat
generate age = "15_25" if age_cat == 1
replace age = "25_35" if age_cat == 2
replace age = "35_45" if age_cat == 3
replace age = "45_100" if age_cat == 4
drop age_cat

outsheet using "`extract_dir'/prepped/hiv_specific_unaids_2017.csv", replace comma names
restore



**************************************************************
**SAVE
**************************************************************
drop if inlist(pubmed_id, 1111, 2222, 3333,28301424) 
save "`regress_dir'/hiv_specific_mort.dta", replace

**************************************************************
**VISUALS
**************************************************************
if `visuals' == 1 {
	preserve
	
		tostring pubmed_id, replace
		generate unique_id = pubmed_id + "_" + id + "_" + sex + "_" + age_group
		rename cum_mort_adj mort_adj
		generate surv = 1-mort
		generate surv_adj = 1-mort_adj
		
		
		pdfstart using "`figure_dir'/compare_mortality_adjustment_hiv.pdf"
		// Compare adjusted mortality with all-cause mortality
		scatter mort_adj mort, mlab(id) xtitle("All-cause proportion dead from KM data") ytitle("Proportion dead due to HIV after adjustment")
		
		
		pdfappend
		
		// Compare mortality curves
		levelsof unique_id, local(ids)
		foreach i of local ids {
			twoway scatter mort_adj yr_since_sc if unique_id == "`i'", mcolor(black) || ///
			scatter mort yr_since_sc if unique_id == "`i'", mcolor(red) xtitle("Years since infection") ytitle("Proportion dying") title("`i'") ylabel(0(0.1)1) xlabel(0(1)12)
			
			pdfappend
		}
			
		// Compare surival curves
		foreach i of local ids {
		
			twoway scatter surv_adj yr_since_sc if unique_id == "`i'" || ///
			scatter surv yr_since_sc if unique_id == "`i'", xtitle("Years since infection") ytitle("Proportion surviving") title("`i'") ylabel(0(0.1)1) xlabel(0(1)12)
			
			pdfappend
		}
		
		pdffinish, view
	restore
	
	
	// Compare UNAIDS weibull and pooled ALPHA/ZAF mienrs cohort survival adjustments and Unaids 2017
	
	preserve
		use `all', clear
		
		keep if inlist(pubmed_id, 1111, 2222, 3333, 28301424)
		replace iso3 = "MINERS" if iso3 == "ZAF"
		generate unique_id = iso3 + "_" + "_" + sex + "_" + age_group
		rename cum_mort_adj mort_adj
		generate surv = 1-mort
		generate surv_adj = 1-mort_adj
		
		pdfstart using "`figure_dir'/compare_mortality_adjustment_unaids.pdf"
		// Compare surival curves
		levelsof unique_id, local(ids)
		foreach i of local ids {
		
			twoway scatter surv_adj yr_since_sc if unique_id == "`i'" || ///
			scatter surv yr_since_sc if unique_id == "`i'", xtitle("Years since infection") ytitle("Proportion surviving") title("`i'") ylabel(0(0.1)1) xlabel(0(1)12)
			
			pdfappend
		}
		pdffinish, view
	restore
		
		
}
