// NAME
// April 2014
// Merge Tim's selected draw numbers to calculate country-specific median survival

**************************************************************
** SET UP
**************************************************************
clear all
set maxvar 20000
set more off
cap restore, not

if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}


local draw_dir "FILEPATH"
local data_dir "FILEPATH"
adopath + "FILEPATH"

get_locations
keep ihme_loc_id location_name region_id region_name super_region_id super_region_name 
rename ihme_loc_id iso3
tempfile codes
save `codes'

// Get list of all countries in draw directory
local countries: dir "`draw_dir'" files "*.csv"

**************************************************************
** FORMAT TIM'S DRAW NUMBERS
**************************************************************
local count = 1
foreach c of local countries {
	di in red "Bringing in `c'"
	insheet using "`draw_dir'/`c'", clear comma names
	local iso3 : subinstr local c "_used_draws.csv" "" 
	generate iso3 = "`iso3'"
	
	if `count' == 1 {
		tempfile appended
		save `appended', replace
	}
	else {
		append using `appended'
		save `appended', replace
	}
	local count = `count' + 1

}

keep no_art_mort_draw iso3
rename no_art_mort_draw draw
replace iso3 = upper(iso3)

expand 2, generate(x)
expand 2, generate(y)
generate age = "15_25" if x == 0 & y == 0
replace age = "25_35" if x == 1 & y == 0
replace age = "35_45" if x == 0 & y == 1
replace age = "45_100" if x == 1 & y == 1
drop x y
tempfile used_draws
save `used_draws', replace

**************************************************************
**MERGE TO MEDIAN SURVIVAL
**************************************************************
insheet using "FILEPATH/median_survival.csv", clear comma names
merge 1:m age draw using `used_draws'
drop _m


**************************************************************
**SAVE FOR GRAPHING IN R
**************************************************************
outsheet using "FILEPATH/matched_draw_median_survival.csv", replace comma names

**************************************************************
**CALCULATE DESCRIPTIVE STATISTICS TO REDUCE DATA SET SIZE
**************************************************************
drop median_surv_lower median_surv_upper median_surv_mean
bysort iso3 age: egen median_surv_mean = mean(median_survival)

sort iso3 age median_survival
by iso3 age: generate median_surv_lower = median_survival[25]
by iso3 age: generate median_surv_upper = median_survival[975]

drop draw median_survival
duplicates drop

tempfile stats
save `stats', replace

**************************************************************
**MERGE ON IDENTIFYING CHARACTERISTICS - REGION, SUPER REGION, HIGH/LOW INCOME, ETC
**************************************************************
merge m:1 iso3 using `codes'
keep if _m == 3
drop _m


order iso3 age median_surv_mean median_surv_lower median_surv_upper
sort iso3 age
outsheet using "FILEPATH/matched_draw_median_survival_stats.csv", replace comma names
