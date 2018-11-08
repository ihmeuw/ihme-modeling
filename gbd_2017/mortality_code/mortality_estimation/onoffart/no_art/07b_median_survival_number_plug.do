// NAME
// April 2014
// Calculate median survival statistics for MDG6 paper

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

**************************************************************
** AVERAGE MEDIAN SURVIVAL IN SOUTHERN AFRICA FOR 25-34 YEAR OLDS
**************************************************************
insheet using "FILEPATH/matched_draw_median_survival_stats.csv", clear  comma names

preserve
keep if age == "25_35"
keep if super_region_name == "Sub-Saharan Africa"
tabstat median_surv_mean, stat(n mean p25 p50 p75 min max)
restore

**************************************************************
** RANGE OF MEDIAN SURVIVAL BY AGE-GROUP, FOR ALL COUNTRIES OR FOR JUST SUB-SAHARAN AFRICA
**************************************************************
tabstat median_surv_mean, by(age) stat(mean p25 p50 p75 range min max)
tabstat median_surv_mean, by(age) stat(mean p25 p50 p75 range min max)


**************************************************************
** AVERAGE MEDIAN SURVIVAL FROM COMPARTMENTAL MODEL FOR 25-34 YEAR OLDS
**************************************************************
insheet using "FILEPATH/median_survival.csv", clear comma names
tab median_surv_mean if age == "25_35"

